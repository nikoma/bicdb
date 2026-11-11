//! Default runnable actual RF3 sparse decision and strict copied cold-open case.
//! In-process data/metadata member delivery is explicit, not TLS qualification.
use super::*;
pub(super) fn make_runtimes(
    fixture: &ActualNativeRf3Fixture,
    providers: &BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
) -> BTreeMap<ClusterNodeId, Arc<RangeRecoveryRuntime>> {
    let registry = Arc::new(Mutex::new(BTreeMap::new()));
    let unavailable = Arc::new(Mutex::new(BTreeSet::from([node("n2")])));
    fixture.transport.set_unavailable(&node("n2"), true);
    fixture
        .services
        .iter()
        .map(|(id, service)| {
            let bus = Arc::new(ObservedRecoveryBus {
                inner: Arc::clone(&fixture.transport),
                caller: providers[id].local_participant(),
                receivers: Arc::clone(&registry),
                unavailable: Arc::clone(&unavailable),
            });
            let runtime = Arc::new(
                RangeRecoveryRuntime::new(Arc::clone(service), Arc::clone(&providers[id]))
                    .with_participant_transport(bus),
            );
            registry.lock().insert(id.clone(), Arc::downgrade(&runtime));
            (id.clone(), runtime)
        })
        .collect()
}
pub(super) fn fence_pair(
    runtimes: &BTreeMap<ClusterNodeId, Arc<RangeRecoveryRuntime>>,
    actor: &MetadataReadParticipant,
    scope: &RecoveryFenceScope,
) {
    for id in [node("n0"), node("n1")] {
        runtimes[&id]
            .fence_epoch(
                actor,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 5000,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
    }
}
#[test]
fn actual_small_sparse_current_quorum_and_strict_cold_reopen_preserve_decision() {
    let mut fixture = ActualNativeRf3Fixture::new();
    // Exercise authenticated native replay for reachable replicas. Drop only
    // Apply1 to n2 to retain the original actual CQ1/native0 precondition;
    // later admission must really repair it rather than project generic rows.
    fixture.transport.enable_native_delivery();
    fixture.transport.drop_native_apply_once(node("n2"), 1);
    fixture.root.disable_cleanup(true);
    eprintln!(
        "SMALL_SPARSE_COLD_ORIGINAL_ROOT {}",
        fixture.root.path().display()
    );
    let range = fixture
        .topology
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    // Inject only message delivery: ACK1 has genuine old quorum n0+n2 while
    // n1 receives NONE of its raw prefix. No caller provides Applied labels.
    fixture.transport.set_unavailable(&node("n1"), true);
    let acknowledged = "retained-prefix-ack-1";
    fixture.append(acknowledged).unwrap();
    let expected = fixture.databases[&node("n0")]
        .read()
        .get("items", acknowledged)
        .unwrap()
        .unwrap();
    let initial_h = fixture.databases[&node("n0")]
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap()
        .logical_history_sha256()
        .to_string();
    let ledger = fixture.root.path().join("controller-acknowledged.jsonl");
    let mut external = std::fs::File::create(&ledger).unwrap();
    writeln!(external,"{}",serde_json::json!({"acknowledged_key":acknowledged,"record_sha256":hex::encode(Sha256::digest(serde_json::to_vec(&expected).unwrap())),"history_after_ack":initial_h})).unwrap();
    external.sync_all().unwrap();
    drop(external);
    assert_eq!(
        fixture.services[&node("n1")]
            .lock()
            .range_write_progress(range.id)
            .last_index,
        0
    );
    assert_eq!(
        fixture.services[&node("n2")]
            .lock()
            .probe_range_write_for_peer(&range, &node("n0"), 1)
            .unwrap()
            .entry
            .unwrap()
            .state,
        RangeWriteState::QuorumCommitted
    );

    BicDb::test_reject_next_native_after_admission();
    let unknown = "retained-prefix-unknown-2";
    let error = fixture.append(unknown).unwrap_err();
    assert!(
        matches!(error, crate::BicDbError::CommitOutcomeUnknown { .. }),
        "{error}"
    );
    let tail = fixture.services[&node("n0")]
        .lock()
        .probe_range_write_for_peer(&range, &node("n0"), 2)
        .unwrap()
        .entry
        .unwrap();
    assert_eq!(tail.state, RangeWriteState::QuorumCommitted);
    let source_native = fixture.databases[&node("n0")]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(source_native.native_applied_index, Some(1));
    assert_eq!(source_native.pending_command_index, Some(2));
    assert_eq!(source_native.materialized_pending_index, None);
    // The old second certifier becomes unavailable only AFTER its real CQ2.
    fixture.transport.set_unavailable(&node("n1"), false);
    fixture.transport.set_unavailable(&node("n2"), true);
    let topology = fixture.topology.read().clone();
    let providers = test_recovery_metadata_cluster(
        &fixture.root.path().join("recovery-metadata"),
        &topology,
        &node("n2"),
    )
    .unwrap();
    let actor = providers[&node("n0")].local_participant();
    let runtimes = make_runtimes(&fixture, &providers);
    let nonce = Uuid::now_v7();
    let role = fresh_role(&providers, &actor, &range, nonce);
    let scope = RecoveryFenceScope::from_role(&role).unwrap();
    drop(role);
    let parent = CancellationToken::uncancelable();
    fence_pair(&runtimes, &actor, &scope);
    let request = RecoveryHistoricalCommandRequest {
        scope: scope.clone(),
        command: tail.command.clone(),
        budget_ms: 5000,
    };
    runtimes[&node("n0")]
        .certify_historical(&actor, &request, &parent)
        .unwrap();
    let before = fixture.services[&node("n1")]
        .lock()
        .test_image_history_obligations(range.id)
        .unwrap();
    assert!(before.irrevocable_commands().is_empty());
    // Actual publication succeeds, but the adapter loses its success response.
    // This is a deterministic filesystem/error cut, not physical power loss.
    let uncertain = runtimes[&node("n1")]
        .test_certify_sparse_historical_with(&actor, &request, &parent, |path, bytes, fsync| {
            assert!(fsync);
            crate::storage::write_atomic(path, bytes, fsync)?;
            Err(invalid(
                "injected after actual durable sparse catalogue publication",
            ))
        })
        .unwrap_err();
    assert!(
        uncertain
            .to_string()
            .contains("injected after actual durable"),
        "{uncertain}"
    );
    assert!(
        runtimes[&node("n1")]
            .observe_history(&actor, &request.probe().unwrap(), &parent)
            .is_err(),
        "uncertain publication cannot issue an observation"
    );
    assert!(
        fixture.services[&node("n1")]
            .lock()
            .test_image_history_obligations(range.id)
            .is_err(),
        "image token cannot come from uncertain catalogue state"
    );
    assert!(
        runtimes[&node("n0")]
            .apply_historical(&actor, &request, &parent)
            .is_err(),
        "uncertain target is not a current cert vote"
    );
    assert_eq!(
        fixture.databases[&node("n0")]
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        Some(1)
    );
    runtimes[&node("n1")]
        .fence_epoch(
            &actor,
            &RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap();
    let sparse_observation = runtimes[&node("n1")]
        .certify_sparse_historical(&actor, &request, &parent)
        .unwrap();
    let sparse = sparse_observation.sparse_decision().unwrap();
    let decision_sha = sparse.checksum_sha256().to_owned();
    let original_open = sparse.native_database_incarnation().to_owned();
    assert_eq!(sparse.command(), &tail.command);
    assert!(sparse_observation.entry().is_none());
    assert_eq!(sparse_observation.fence().progress().last_index, 0);
    assert_eq!(
        sparse_observation.fence().native().native_applied_index,
        Some(0)
    );
    let after = fixture.services[&node("n1")]
        .lock()
        .test_image_history_obligations(range.id)
        .unwrap();
    assert_eq!(
        after.irrevocable_commands(),
        &BTreeMap::from([(2, tail.command.checksum_sha256.clone())])
    );
    assert!(fixture.services[&node("n1")]
        .lock()
        .test_require_image_history_obligations_current(&before)
        .is_err());
    // Decoder-only negatives. No edited DTO is promoted to an observation.
    let encoded = serde_json::to_value(sparse).unwrap();
    for field in ["command", "native_birth_id"] {
        let mut missing = encoded.clone();
        missing["record"].as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<
            crate::distribution_range_consensus::SparseHistoricalDecisionProjection,
        >(missing)
        .is_err());
    }
    let mut missing = encoded;
    missing
        .as_object_mut()
        .unwrap()
        .remove("observed_native_database_incarnation");
    assert!(serde_json::from_value::<
        crate::distribution_range_consensus::SparseHistoricalDecisionProjection,
    >(missing)
    .is_err());
    let applied = runtimes[&node("n0")]
        .apply_historical(&actor, &request, &parent)
        .unwrap();
    assert_eq!(applied.fence().native().native_applied_index, Some(2));
    assert_eq!(applied.fence().progress().resolved_through, 2);
    assert_eq!(
        fixture.databases[&node("n0")]
            .read()
            .get("items", acknowledged)
            .unwrap(),
        Some(expected.clone())
    );
    assert_eq!(
        std::fs::read_to_string(&ledger).unwrap().lines().count(),
        1,
        "unknown2 is never an ACK"
    );
    wait_for_contexts(&runtimes);
    let original_root = fixture.root.path().to_path_buf();
    let weak = fixture
        .databases
        .values()
        .map(Arc::downgrade)
        .collect::<Vec<_>>();
    drop(sparse_observation);
    drop(applied);
    drop(after);
    drop(before);
    drop(runtimes);
    drop(providers);
    drop(fixture);
    let close_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while weak.iter().any(|db| db.upgrade().is_some()) && std::time::Instant::now() < close_deadline
    {
        // A completed metadata request can still be unwinding its last owner.
        // Wait outside every DB/service latch; never clear or bypass ownership.
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(
        weak.iter().all(|db| db.upgrade().is_none()),
        "all original DB owners must close before copying"
    );
    let copied = ActualNativeRf3Fixture::copy_stopped_recovery_tree(&original_root).unwrap();
    let mut reopened = ActualNativeRf3Fixture::reopen_copied_recovery_state(copied).unwrap();
    reopened.root.disable_cleanup(true);
    eprintln!(
        "SMALL_SPARSE_COLD_COPIED_ROOT {}",
        reopened.root.path().display()
    );
    assert_eq!(
        reopened.databases[&node("n0")]
            .read()
            .get("items", acknowledged)
            .unwrap(),
        Some(expected)
    );
    assert_eq!(
        reopened.databases[&node("n1")]
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        Some(0)
    );
    let topology = reopened.topology.read().clone();
    let providers = test_recovery_metadata_cluster(
        &reopened.root.path().join("recovery-metadata"),
        &topology,
        &node("n2"),
    )
    .unwrap();
    let actor = providers[&node("n0")].local_participant();
    let runtimes = make_runtimes(&reopened, &providers);
    fence_pair(&runtimes, &actor, &scope);
    let cold = runtimes[&node("n1")]
        .observe_history(&actor, &request.probe().unwrap(), &parent)
        .unwrap();
    let cold_sparse = cold.sparse_decision().unwrap();
    assert_eq!(cold_sparse.checksum_sha256(), decision_sha);
    assert_ne!(cold_sparse.native_database_incarnation(), original_open);
    assert_eq!(
        cold_sparse.native_database_incarnation(),
        cold.fence().native_database_incarnation()
    );
    assert_eq!(cold.fence().progress().last_index, 0);
    assert_eq!(cold.fence().native().native_applied_index, Some(0));
    let role = fresh_role(&providers, &actor, &range, Uuid::now_v7());
    let renewed_scope = RecoveryFenceScope::from_role(&role).unwrap();
    drop(role);
    fence_pair(&runtimes, &actor, &renewed_scope);
    let renewed = RecoveryHistoricalCommandRequest {
        scope: renewed_scope.clone(),
        command: tail.command.clone(),
        budget_ms: 5000,
    };
    let recertified = runtimes[&node("n1")]
        .certify_sparse_historical(&actor, &renewed, &parent)
        .unwrap();
    assert_eq!(
        recertified.sparse_decision().unwrap().checksum_sha256(),
        decision_sha,
        "new nonce preserves original committed audit/checksum"
    );
    assert_eq!(
        recertified.current_certification().unwrap().recovery_nonce,
        renewed_scope.recovery_nonce
    );
    assert_eq!(recertified.fence().progress().last_index, 0);
    assert!(runtimes[&node("n1")]
        .prepare_native_ready(
            &actor,
            &NativeReadyRequest {
                scope: renewed_scope.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 5000
            },
            &parent
        )
        .unwrap_err()
        .to_string()
        .contains("sparse history is not fully materialized"));
    // Explicit wrong-backing negative: create another REAL physical birth with
    // the same member/bootstrap/schema and place only an inert copied logical
    // catalogue beside it. No edited DTO or forged admission is used.
    let mut wrong_root = tempfile::tempdir().unwrap();
    wrong_root.disable_cleanup(true);
    let wrong_path = wrong_root.path().join("different-birth-n1");
    let spec = reopened.specs[&node("n1")].clone();
    let config = crate::DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(crate::StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let (mut wrong_db, _) =
        BicDb::create_new_range_image_database(&wrong_path, config.clone(), spec.clone()).unwrap();
    wrong_db.create_collection("items").unwrap();
    drop(wrong_db);
    let (wrong_db, genesis) = BicDb::open_range_image_database(&wrong_path, config, &spec).unwrap();
    // The new backing must pass the ordinary actual reopened schema gate
    // before this negative isolates its distinct birth identity.
    let observed_schema = wrong_db.schema_compatibility_fingerprint().unwrap().sha256;
    assert_eq!(
        topology.nodes[&node("n1")].labels[crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL],
        observed_schema
    );
    eprintln!(
        "SMALL_SPARSE_WRONG_BIRTH_ROOT {}",
        wrong_root.path().display()
    );
    let wrong_db = Arc::new(parking_lot::RwLock::new(wrong_db));
    let gate = wrong_db.read().range_mutation_gate();
    let freeze = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
    BicDb::initialize_range_image_genesis(
        &wrong_db,
        &genesis,
        &range,
        &freeze,
        Duration::from_secs(3),
        &parent,
    )
    .unwrap();
    let native = wrong_db
        .read()
        .native_recovery_admission(
            &topology.cluster_id,
            spec.bootstrap_id,
            &node("n1"),
            spec.member_incarnation,
        )
        .unwrap();
    let real_native = reopened.databases[&node("n1")]
        .read()
        .native_recovery_admission(
            &topology.cluster_id,
            spec.bootstrap_id,
            &node("n1"),
            spec.member_incarnation,
        )
        .unwrap();
    assert_ne!(native.birth_id(), real_native.birth_id());
    let catalog_name = crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG;
    let copied_catalog = std::fs::read(reopened.root.path().join("n1").join(catalog_name)).unwrap();
    crate::storage::write_atomic(&wrong_path.join(catalog_name), &copied_catalog, true).unwrap();
    let mut wrong_store = crate::distribution_range_consensus::RangeWriteStore::open(
        &wrong_path,
        topology.cluster_id.clone(),
        node("n1"),
        true,
    )
    .unwrap();
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
    let observed = providers[&node("n1")]
        .witness_recovery_role(
            &actor,
            business,
            range.id,
            range.epoch,
            renewed_scope.recovery_nonce,
            challenge,
            purpose.clone(),
            &parent,
        )
        .unwrap();
    let wrong_role = RecoveryRoleAdmission::from_witness(
        observed,
        &actor,
        &node("n1"),
        business,
        range.id,
        range.epoch,
        renewed_scope.recovery_nonce,
        challenge,
        &purpose,
    )
    .unwrap();
    let wrong_scope = RecoveryFenceScope::from_role(&wrong_role).unwrap();
    gate.validate_quiescent_frozen(range.id, &freeze).unwrap();
    native.require_current(&wrong_db.read()).unwrap();
    let wrong_error = wrong_store
        .fence_recovery_epoch(&wrong_role, &native, &wrong_scope)
        .unwrap_err();
    assert!(
        wrong_error
            .to_string()
            .contains("another actual native birth/restore"),
        "{wrong_error}"
    );
    assert_eq!(
        std::fs::read(wrong_path.join(catalog_name)).unwrap(),
        copied_catalog,
        "wrong stable birth must refuse BEFORE persistence or catalogue changes"
    );
    assert_eq!(
        wrong_db
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        Some(0)
    );
    std::fs::write(wrong_root.path().join("wrong-birth-refusal.json"), serde_json::to_vec_pretty(
        &serde_json::json!({"expected_birth":real_native.birth_id(),"actual_distinct_birth":native.birth_id(),"error":wrong_error.to_string(),"catalogue_bytes_unchanged":true})
    ).unwrap()).unwrap();
    eprintln!(
        "SMALL_SPARSE_COLD_WRONG_BIRTH_ROOT {}",
        wrong_root.path().display()
    );
    wait_for_contexts(&runtimes);
    let report = serde_json::json!({"classification":"ACTUAL_SPARSE_CURRENT_QUORUM_AND_COLD_REOPEN","original_root":original_root,"copied_root":reopened.root.path(),"source_native_after_actual_apply":2,"behind_raw_native":0,"stable_sparse_sha256":decision_sha,"original_observed_open":original_open,"cold_observed_open":cold_sparse.native_database_incarnation(),"actual_postpublication_error_refused_observations":true,"actual_same_session_refsync":true,"new_nonce_preserved_old_full_decision":true,"image_token_includes_sparse_and_stales":true,"missing_complete_command_or_birth_or_open_fields_rejected":true,"acknowledged_values_reconciled":1,"unknown_calls":1,"new_application_ack_tested":false,"physical_qualification":false,"strong_get_tested":false});
    std::fs::write(
        reopened.root.path().join("small-sparse-cold-report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("SMALL_SPARSE_COLD_RESULT {report}");
}
