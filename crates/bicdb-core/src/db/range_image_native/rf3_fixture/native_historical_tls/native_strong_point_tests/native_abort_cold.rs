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
    let candidate = fixture.clients[&node("n0")]
        .observe_native_abort_candidate(&node("n0"), &request, &super::native_abort_tests::token())
        .unwrap();
    let request = crate::distribution_range_recovery::NativeAbortRequest {
        candidate_sha256: Some(candidate.sha256().into()),
        ..request
    };
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&node("n0")]
            .promise_native_abort(&id, &request, &super::native_abort_tests::token())
            .unwrap();
        fixture.idle();
    }
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

// ---------------------------------------------------------------------------
// Reopened origin: a Prepared-only slot left by an EARLIER open.
// ---------------------------------------------------------------------------
use crate::distribution_range_consensus::native_abort::{
    test_arm_native_admission_stop, test_native_admission_stop_armed, TestNativeAdmissionStop,
};

/// An append on `range` whose admission stops at `point` as if the process
/// ended there. Returns the key and the origin's raw command for it. The
/// stopped attempt writes no application WAL.
fn stopped_append(
    fixture: &EnforcedNetwork,
    range: &RangeDescriptor,
    point: TestNativeAdmissionStop,
    prefix: &str,
) -> (String, RangeWriteCommand) {
    let key = fixture.key(range.id, prefix);
    let before = fixture.native.databases[&node("n0")]
        .read()
        .tx_log_handle()
        .stats();
    test_arm_native_admission_stop(range.id, point);
    let error = fixture.native.append(&key).unwrap_err();
    assert!(!test_native_admission_stop_armed(), "{error}");
    assert!(
        error.to_string().contains("injected admission stop"),
        "{error}"
    );
    fixture.idle();
    let after = fixture.native.databases[&node("n0")]
        .read()
        .tx_log_handle()
        .stats();
    assert_eq!(after.commits_written, before.commits_written);
    assert_eq!(after.written_seq, before.written_seq);
    let service = fixture.native.services[&node("n0")].lock();
    let progress = service.range_write_progress(range.id);
    let entry = service
        .probe_range_write_for_peer(range, &node("n0"), progress.last_index)
        .unwrap()
        .entry
        .unwrap();
    (key, entry.command)
}
fn current_range(fixture: &EnforcedNetwork, range: RangeId) -> RangeDescriptor {
    fixture
        .native
        .topology
        .read()
        .range_by_id(range)
        .unwrap()
        .clone()
}
fn raw_state(
    fixture: &EnforcedNetwork,
    id: &ClusterNodeId,
    range: &RangeDescriptor,
    index: u64,
) -> Option<crate::distribution_range_consensus::RangeWriteState> {
    fixture.native.services[id]
        .lock()
        .probe_range_write_for_peer(range, &node("n0"), index)
        .unwrap()
        .entry
        .map(|e| e.state)
}
/// Resolution at the origin first (it creates the promises), then at each
/// follower; then preparation, completion and activation, as the cold
/// driver above does after its reopen.
fn drive_abort(fixture: &EnforcedNetwork, range: &RangeDescriptor, index: u64, candidate: &str) {
    let scope = fixture.actual_current_recovery_scope(range);
    let request = crate::distribution_range_recovery::NativeAbortRequest {
        scope,
        range_index: index,
        origin_node_id: node("n0"),
        candidate_sha256: None,
        challenge: Uuid::now_v7(),
        budget_ms: 4500,
    };
    // The consumer accepts the reopened producer's record as the candidate.
    let observed = fixture.clients[&node("n0")]
        .observe_native_abort_candidate(&node("n0"), &request, &super::native_abort_tests::token())
        .unwrap();
    assert_eq!(observed.sha256(), candidate);
    let request = crate::distribution_range_recovery::NativeAbortRequest {
        candidate_sha256: Some(candidate.into()),
        ..request
    };
    for id in [node("n0"), node("n1"), node("n2")] {
        let (r, _, _) = fixture.clients[&node("n0")]
            .resolve_native_abort_at(
                &id,
                &crate::distribution_range_recovery::NativeAbortRequest {
                    challenge: Uuid::now_v7(),
                    ..request.clone()
                },
                &super::native_abort_tests::token(),
            )
            .unwrap();
        assert_eq!(r, index);
        fixture.idle();
    }
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&node("n0")]
            .prepare_native_ready_observation(
                &id,
                &crate::distribution_range_recovery::NativeReadyRequest {
                    scope: request.scope.clone(),
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
                    scope: request.scope.clone(),
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
                    scope: request.scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4500,
                },
                &super::native_abort_tests::token(),
            )
            .unwrap();
        fixture.idle();
    }
}

/// Prepared-only tail (followers prepared too) left by the earlier open: the
/// reopened origin produces its Core no-effect intent, the unchanged abort
/// protocol aborts the slot, the range accepts writes again and no
/// acknowledged row is lost.
#[test]
fn native_abort_reopened_prepared_only_tail_aborts_and_serves() {
    let fixture = EnforcedNetwork::new();
    let range = fixture.ranges()[0].clone();
    let acked = fixture.key(range.id, "acked-before-prepared-only");
    fixture.native.append(&acked).unwrap();
    let (lost, command) = stopped_append(
        &fixture,
        &range,
        TestNativeAdmissionStop::AfterPrepareQuorum,
        "prepared-only-never-acked",
    );
    assert_eq!(command.index, 2);
    for id in [node("n0"), node("n1"), node("n2")] {
        assert_eq!(
            raw_state(&fixture, &id, &range, 2),
            Some(crate::distribution_range_consensus::RangeWriteState::Prepared),
            "{id}"
        );
    }
    let mut fixture = reopen_stopped(fixture);
    let range = current_range(&fixture, range.id);
    let candidate = fixture.native.services[&node("n0")]
        .lock()
        .reopened_native_abort_candidate(&range, &command)
        .unwrap();
    // Idempotent: a later attempt reuses the same durable intent.
    assert_eq!(
        fixture.native.services[&node("n0")]
            .lock()
            .reopened_native_abort_candidate(&range, &command)
            .unwrap(),
        candidate
    );
    drive_abort(&fixture, &range, 2, &candidate);
    fixture.coordinators[&node("n0")]
        .recover_local_leadership(8)
        .unwrap();
    let next = fixture.key(range.id, "acked-after-reopened-abort");
    fixture.native.append(&next).unwrap();
    for id in [node("n0"), node("n1"), node("n2")] {
        let db = fixture.native.databases[&id].read();
        assert!(db.get("items", &acked).unwrap().is_some(), "{id}");
        assert!(db.get("items", &next).unwrap().is_some(), "{id}");
        assert!(db.get("items", &lost).unwrap().is_none(), "{id}");
        assert_eq!(
            db.current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            3
        );
    }
    fixture.native.root.disable_cleanup(false);
    fixture._keys.disable_cleanup(false);
}

/// The reopened producer refuses every state it is not made for.
#[test]
fn native_abort_reopened_no_effect_producer_refuses_other_states() {
    let fixture = EnforcedNetwork::new();
    let ranges = fixture.ranges();
    let (range, committed_range) = (ranges[0].clone(), ranges[1].clone());
    let (_, command) = stopped_append(
        &fixture,
        &range,
        TestNativeAdmissionStop::AfterPrepareQuorum,
        "prepared-in-this-open",
    );
    // (1) A slot started by THIS open is never taken for an earlier one.
    let error = fixture.native.services[&node("n0")]
        .lock()
        .reopened_native_abort_candidate(&range, &command)
        .unwrap_err();
    assert!(
        error.to_string().contains("started by this open"),
        "{error}"
    );
    // (2) Local Commit vote: ever-Commit set and raw Committed.
    let (_, committed) = stopped_append(
        &fixture,
        &committed_range,
        TestNativeAdmissionStop::AfterLocalCommit,
        "committed-locally",
    );
    assert_eq!(
        raw_state(&fixture, &node("n0"), &committed_range, committed.index),
        Some(crate::distribution_range_consensus::RangeWriteState::Committed)
    );
    let mut fixture = reopen_stopped(fixture);
    let range = current_range(&fixture, range.id);
    let committed_range = current_range(&fixture, committed_range.id);
    let error = fixture.native.services[&node("n0")]
        .lock()
        .reopened_native_abort_candidate(&committed_range, &committed)
        .unwrap_err();
    assert!(
        error.to_string().contains("exactly one Prepared"),
        "{error}"
    );
    {
        // The Core producer alone also refuses it, on the ever-Commit fact.
        let db = fixture.native.databases[&node("n0")].read();
        let permit = db
            .range_mutation_gate()
            .try_enter(committed_range.id)
            .unwrap();
        let error = db
            .persist_reopened_native_no_effect(
                &committed,
                &node("n0"),
                &committed_range,
                &crate::distribution_range_mutation::RangeMutationAuthority::Mutation(&permit),
            )
            .unwrap_err();
        assert!(error.to_string().contains("conflicts"), "{error}");
    }
    // (3) Wrong index: no Prepared first unresolved entry there.
    let mut wrong = command.clone();
    wrong.index += 1;
    assert!(fixture.native.services[&node("n0")]
        .lock()
        .reopened_native_abort_candidate(&range, &wrong)
        .is_err());
    // (4) Not this node's command: a follower holding the same slot.
    let error = fixture.native.services[&node("n1")]
        .lock()
        .reopened_native_abort_candidate(&range, &command)
        .unwrap_err();
    assert!(
        error.to_string().contains("own same-epoch command"),
        "{error}"
    );
    // (5) The matching state succeeds once; after that the Core producer
    // refuses (intent present), and so it does once a promise exists.
    let candidate = fixture.native.services[&node("n0")]
        .lock()
        .reopened_native_abort_candidate(&range, &command)
        .unwrap();
    let scope = fixture.actual_current_recovery_scope(&range);
    let request = crate::distribution_range_recovery::NativeAbortRequest {
        scope,
        range_index: command.index,
        origin_node_id: node("n0"),
        candidate_sha256: Some(candidate),
        challenge: Uuid::now_v7(),
        budget_ms: 4500,
    };
    fixture.clients[&node("n0")]
        .promise_native_abort(&node("n0"), &request, &super::native_abort_tests::token())
        .unwrap();
    fixture.idle();
    let error = fixture.native.services[&node("n0")]
        .lock()
        .reopened_native_abort_candidate(&range, &command)
        .unwrap_err();
    assert!(error.to_string().contains("promise"), "{error}");
    fixture.native.root.disable_cleanup(false);
    fixture._keys.disable_cleanup(false);
}

/// A follower holds an abort promise for the slot. Only the origin's abort
/// round creates one, from the origin's Core no-effect intent, and that
/// intent and the ever-Commit mark exclude each other at the origin. So the
/// origin is not ever-Commit: its own Commit vote and the Commit re-drive are
/// refused, and the follower's unchanged Commit receiver refuses the
/// original Commit as well. No member is marked ever-Commit.
#[test]
fn native_abort_reopened_follower_promise_refuses_commit_redrive() {
    use crate::distribution_range_consensus::RangeWriteTransport;
    let fixture = EnforcedNetwork::new();
    let range = fixture.ranges()[0].clone();
    let (_, command) = stopped_append(
        &fixture,
        &range,
        TestNativeAdmissionStop::AfterPrepareQuorum,
        "prepared-then-follower-promise",
    );
    let mut fixture = reopen_stopped(fixture);
    let range = current_range(&fixture, range.id);
    let candidate = fixture.native.services[&node("n0")]
        .lock()
        .reopened_native_abort_candidate(&range, &command)
        .unwrap();
    let scope = fixture.actual_current_recovery_scope(&range);
    let request = crate::distribution_range_recovery::NativeAbortRequest {
        scope,
        range_index: command.index,
        origin_node_id: node("n0"),
        candidate_sha256: Some(candidate),
        challenge: Uuid::now_v7(),
        budget_ms: 4500,
    };
    fixture.clients[&node("n0")]
        .promise_native_abort(&node("n1"), &request, &super::native_abort_tests::token())
        .unwrap();
    fixture.idle();
    let error = fixture.native.services[&node("n0")]
        .lock()
        .reopened_native_commit_vote(&range, &command)
        .unwrap_err();
    assert!(error.to_string().contains("not ever-Commit"), "{error}");
    let error = fixture.coordinators[&node("n0")]
        .redrive_reopened_commit_votes(&command)
        .unwrap_err();
    assert!(error.to_string().contains("not ever-Commit"), "{error}");
    let error = fixture.clients[&node("n0")]
        .commit_range_write(&node("n1"), &command)
        .unwrap_err();
    eprintln!("FOLLOWER_PROMISE_COMMIT_REFUSAL {error}");
    assert!(
        error.to_string().contains("AbortPromise")
            || error.to_string().contains("no-effect/abort slot")
            || error.to_string().contains("active recovery")
            // The promise round froze the follower's mutation gate for its
            // session; the Commit receiver refuses before any write.
            || error.to_string().contains("is frozen, draining, or invalid"),
        "{error}"
    );
    fixture.idle();
    for id in [node("n0"), node("n1"), node("n2")] {
        let db = fixture.native.databases[&id].read();
        let slot = db
            .native_abort_slot_inventory()
            .unwrap()
            .into_iter()
            .find(|s| s.command.range_id == range.id && s.command.index == command.index)
            .unwrap();
        assert!(!slot.ever_commit, "{id}");
        assert_eq!(
            slot.abort_promise_sha256.is_some(),
            id == node("n1"),
            "{id}"
        );
    }
    fixture.native.root.disable_cleanup(false);
    fixture._keys.disable_cleanup(false);
}

/// Followers holding Commit for the slot (which no real origin can cause:
/// it marks ever-Commit before sending Commit): the abort is refused with
/// Irreversible and no follower promises it. A native Pending at the origin
/// (on the other range) is refused by the producer.
#[test]
fn native_abort_reopened_follower_commit_evidence_refuses_abort() {
    let fixture = EnforcedNetwork::new();
    let ranges = fixture.ranges();
    let (range, pending_range) = (ranges[0].clone(), ranges[1].clone());
    let (_, command) = stopped_append(
        &fixture,
        &range,
        TestNativeAdmissionStop::AfterPrepareQuorum,
        "prepared-then-followers-commit",
    );
    // A native Pending with no application WAL on the other range.
    let pending_key = fixture.key(pending_range.id, "pending-not-applied");
    crate::BicDb::test_reject_next_native_after_admission();
    assert!(fixture.native.append(&pending_key).is_err());
    fixture.idle();
    let pending = fixture.native.databases[&node("n0")]
        .read()
        .native_range_prefix_observation(pending_range.id)
        .unwrap();
    assert!(pending.pending_command_index.is_some(), "{pending:?}");
    let pending_index = pending.pending_command_index.unwrap();
    let pending_command = fixture.native.services[&node("n0")]
        .lock()
        .probe_range_write_for_peer(&pending_range, &node("n0"), pending_index)
        .unwrap()
        .entry
        .unwrap()
        .command;
    let mut fixture = reopen_stopped(fixture);
    let range = current_range(&fixture, range.id);
    let pending_range = current_range(&fixture, pending_range.id);
    assert!(fixture.native.services[&node("n0")]
        .lock()
        .reopened_native_abort_candidate(&pending_range, &pending_command)
        .is_err());
    {
        let db = fixture.native.databases[&node("n0")].read();
        let permit = db
            .range_mutation_gate()
            .try_enter(pending_range.id)
            .unwrap();
        assert!(db
            .persist_reopened_native_no_effect(
                &pending_command,
                &node("n0"),
                &pending_range,
                &crate::distribution_range_mutation::RangeMutationAuthority::Mutation(&permit),
            )
            .is_err());
    }
    let candidate = fixture.native.services[&node("n0")]
        .lock()
        .reopened_native_abort_candidate(&range, &command)
        .unwrap();
    for id in [node("n1"), node("n2")] {
        fixture.native.services[&id]
            .lock()
            .commit_range_write(&range, &node("n0"), &command)
            .unwrap();
    }
    let scope = fixture.actual_current_recovery_scope(&range);
    let request = crate::distribution_range_recovery::NativeAbortRequest {
        scope,
        range_index: command.index,
        origin_node_id: node("n0"),
        candidate_sha256: Some(candidate),
        challenge: Uuid::now_v7(),
        budget_ms: 4500,
    };
    let error = fixture.clients[&node("n0")]
        .resolve_native_abort_at(&node("n0"), &request, &super::native_abort_tests::token())
        .unwrap_err();
    assert!(error.to_string().contains("irreversible"), "{error}");
    fixture.idle();
    for id in [node("n1"), node("n2")] {
        assert_eq!(
            raw_state(&fixture, &id, &range, command.index),
            Some(crate::distribution_range_consensus::RangeWriteState::Committed),
            "{id}"
        );
        assert!(fixture.native.databases[&id]
            .read()
            .observe_actual_native_abort_promise(range.id, command.index)
            .unwrap()
            .is_none());
    }
    fixture.native.root.disable_cleanup(false);
    fixture._keys.disable_cleanup(false);
}

/// The no-effect intent record is published before the slot's
/// `no_effect_sha256` is reconciled. A durable intent whose slot field is not
/// reconciled yet still refuses the ever-Commit mark.
#[test]
fn native_abort_reopened_unreconciled_intent_refuses_ever_commit_mark() {
    let fixture = EnforcedNetwork::new();
    let range = fixture.ranges()[0].clone();
    let (_, command) = stopped_append(
        &fixture,
        &range,
        TestNativeAdmissionStop::AfterPrepareQuorum,
        "prepared-then-unreconciled-intent",
    );
    let mut fixture = reopen_stopped(fixture);
    let range = current_range(&fixture, range.id);
    {
        let db = fixture.native.databases[&node("n0")].read();
        let permit = db.range_mutation_gate().try_enter(range.id).unwrap();
        let authority =
            crate::distribution_range_mutation::RangeMutationAuthority::Mutation(&permit);
        crate::BicDb::test_stop_next_no_effect_after_intent();
        let error = db
            .persist_reopened_native_no_effect(&command, &node("n0"), &range, &authority)
            .unwrap_err();
        assert!(error.to_string().contains("injected stop"), "{error}");
        let slot = db
            .native_abort_slot_inventory()
            .unwrap()
            .into_iter()
            .find(|s| s.command == command)
            .unwrap();
        assert!(slot.no_effect_sha256.is_none());
        assert!(!slot.ever_commit);
        let error = db
            .native_abort_mark_ever_commit(&command, &range, &authority)
            .unwrap_err();
        assert!(
            error.to_string().contains("durable no-effect intent"),
            "{error}"
        );
        let slot = db
            .native_abort_slot_inventory()
            .unwrap()
            .into_iter()
            .find(|s| s.command == command)
            .unwrap();
        assert!(!slot.ever_commit);
    }
    fixture.native.root.disable_cleanup(false);
    fixture._keys.disable_cleanup(false);
}
