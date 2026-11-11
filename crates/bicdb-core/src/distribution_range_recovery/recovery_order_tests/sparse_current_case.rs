//! Manual external-artifact case. The preserved root is READ/COPIED only;
//! native and metadata engines open a new owned TempDir, never the original.
use super::*;

#[test]
#[ignore = "requires explicit BICDB_RETAINED_NATIVE_RECOVERY_ROOT; copies retained actual 1025-ACK fixture"]
fn copied_actual_1025_prefix_sparse_current_quorum_finishes_native_tail() {
    let retained = std::env::var_os("BICDB_RETAINED_NATIVE_RECOVERY_ROOT")
        .expect("explicit preserved test-artifact root is required");
    let source = std::path::PathBuf::from(retained);
    assert!(source.join("first-recovery-refusal.json").is_file());
    assert!(source.join("controller-acknowledged-1025.jsonl").is_file());
    let original_catalog = std::fs::read(source.join("n0/cluster-range-write-log.json")).unwrap();
    let original_refusal = std::fs::read(source.join("first-recovery-refusal.json")).unwrap();
    let original_scope: serde_json::Value = serde_json::from_slice(&original_catalog).unwrap();
    // This is an UNTRUSTED REQUEST, never an authority. Every call below obtains
    // independently fresh actual metadata witness and validates the real native
    // birth/new DB-open before persisting the exact-session fence again.
    let scope: RecoveryFenceScope =
        serde_json::from_value(original_scope["catalog"]["recovery_fences"]["1"]["scope"].clone())
            .unwrap();
    let copied = ActualNativeRf3Fixture::copy_stopped_recovery_tree(&source).unwrap();
    let mut fixture = ActualNativeRf3Fixture::reopen_copied_recovery_state(copied).unwrap();
    fixture.root.disable_cleanup(true);
    eprintln!(
        "SPARSE_CURRENT_CERTIFICATION_RETAINED_ROOT {}",
        fixture.root.path().display()
    );
    assert_ne!(
        fixture.root.path().canonicalize().unwrap(),
        source.canonicalize().unwrap()
    );
    let range = fixture
        .topology
        .read()
        .range_by_id(scope.range_id)
        .unwrap()
        .clone();
    let initial_native = fixture
        .databases
        .iter()
        .map(|(id, db)| {
            (
                id.clone(),
                db.read()
                    .native_range_prefix_observation(scope.range_id)
                    .unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(initial_native[&node("n0")].native_applied_index, Some(1025));
    assert_eq!(
        initial_native[&node("n0")].pending_command_index,
        Some(1026)
    );
    assert_eq!(initial_native[&node("n0")].materialized_pending_index, None);
    assert_eq!(initial_native[&node("n1")].native_applied_index, Some(0));
    let source_progress = fixture.services[&node("n0")]
        .lock()
        .range_write_progress(scope.range_id);
    assert_eq!(
        (
            source_progress.last_index,
            source_progress.resolved_through,
            source_progress.compacted_through
        ),
        (1026, 1025, 1)
    );
    assert_eq!(
        fixture.services[&node("n1")]
            .lock()
            .range_write_progress(scope.range_id)
            .last_index,
        0
    );
    fixture.transport.set_unavailable(&node("n2"), true);
    let topology = fixture.topology.read().clone();
    let providers = test_recovery_metadata_cluster(
        &fixture.root.path().join("recovery-metadata"),
        &topology,
        &node("n2"),
    )
    .unwrap();
    let initiator = providers[&node("n0")].local_participant();
    let registry = Arc::new(Mutex::new(BTreeMap::new()));
    let unavailable = Arc::new(Mutex::new(BTreeSet::from([node("n2")])));
    let runtimes = fixture
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
        .collect::<BTreeMap<_, _>>();
    let parent = CancellationToken::uncancelable();
    let mut observations = BTreeMap::new();
    for id in [node("n0"), node("n1")] {
        let actual = runtimes[&id]
            .fence_epoch(
                &initiator,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 5000,
                },
                &parent,
            )
            .unwrap();
        observations.insert(id, actual);
    }
    // Every actual obligation remains present. The source CQ tail is not
    // truncated, relabelled Applied or assumed covered by the native point.
    let history = observations[&node("n0")].image_history();
    assert_eq!(
        history
            .irreversible_command_sha256
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        (2..=1026).collect::<Vec<_>>()
    );
    assert_eq!(history.native_point.as_ref().unwrap().range_index, 1025);
    assert_eq!(history.pending_native.as_ref().unwrap().index, 1026);
    assert!(observations[&node("n1")]
        .image_history()
        .irreversible_command_sha256
        .is_empty());
    let limits = RangeWriteRepairLimits::default();
    let prefix_error = fixture.services[&node("n0")]
        .lock()
        .export_range_write_repair(&range, &node("n0"), 0, &limits)
        .unwrap_err();
    assert!(
        prefix_error
            .to_string()
            .contains("requires a range snapshot"),
        "{prefix_error}"
    );
    let tail = fixture.services[&node("n0")]
        .lock()
        .probe_range_write_for_peer(&range, &node("n0"), 1026)
        .unwrap()
        .entry
        .unwrap();
    assert_eq!(tail.state, RangeWriteState::QuorumCommitted);
    let request = RecoveryHistoricalCommandRequest {
        scope: scope.clone(),
        command: tail.command.clone(),
        budget_ms: 5000,
    };
    runtimes[&node("n0")]
        .certify_historical(&initiator, &request, &parent)
        .unwrap();
    let tail_gap = runtimes[&node("n1")]
        .certify_historical(&initiator, &request, &parent)
        .unwrap_err();
    assert!(
        tail_gap.to_string().contains("compacted/unavailable gap"),
        "{tail_gap}"
    );
    // Pure gap refusal must also leave the repaired validation path usable;
    // no second manual refsync or direct uncertainty reset is allowed here.
    runtimes[&node("n1")]
        .observe_history(
            &initiator,
            &RecoveryFencedProbeRequest {
                scope: scope.clone(),
                index: Some(1026),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap();
    // A separate protocol operation persists the actual committed decision on
    // the lagging voter. No row, ordered log entry, or applied floor advances.
    let before_image_obligations = fixture.services[&node("n1")]
        .lock()
        .test_image_history_obligations(scope.range_id)
        .unwrap();
    assert!(before_image_obligations.irrevocable_commands().is_empty());
    let sparse_observation = runtimes[&node("n1")]
        .certify_sparse_historical(&initiator, &request, &parent)
        .unwrap();
    assert!(sparse_observation.entry().is_none());
    let sparse = sparse_observation.sparse_decision().unwrap();
    assert_eq!(sparse.command(), &tail.command);
    assert_eq!(sparse_observation.fence().progress().last_index, 0);
    assert_eq!(sparse_observation.fence().progress().resolved_through, 0);
    assert_eq!(
        sparse_observation.fence().native().native_applied_index,
        Some(0)
    );
    assert_eq!(sparse_observation.fence().image_history().format_version, 2);
    assert_eq!(
        sparse_observation
            .fence()
            .image_history()
            .irreversible_command_sha256
            .get(&1026),
        Some(&tail.command.checksum_sha256)
    );
    let after_image_obligations = fixture.services[&node("n1")]
        .lock()
        .test_image_history_obligations(scope.range_id)
        .unwrap();
    assert_eq!(
        after_image_obligations.irrevocable_commands(),
        &BTreeMap::from([(1026, tail.command.checksum_sha256.clone())])
    );
    assert_eq!(after_image_obligations.last_index(), 0);
    assert!(fixture.services[&node("n1")].lock()
        .test_require_image_history_obligations_current(&before_image_obligations).is_err(),
        "new sparse history must invalidate an actual old image token even with unchanged raw counters");
    let source_applied = runtimes[&node("n0")]
        .apply_historical(&initiator, &request, &parent)
        .unwrap();
    assert_eq!(source_applied.fence().progress().resolved_through, 1026);
    assert_eq!(
        source_applied.fence().native().native_applied_index,
        Some(1026)
    );
    let frozen = runtimes[&node("n0")].source_capture_freeze(&scope).unwrap();
    let role = fresh_role(&providers, &initiator, &range, scope.recovery_nonce);
    let source_ready = fixture.services[&node("n0")]
        .lock()
        .attest_native_recovery_ready(role, frozen, &parent)
        .unwrap();
    assert_eq!(source_ready.range_index(), 1026);
    // Sparse decision storage alone still grants no native readiness.
    let behind_ready_error = runtimes[&node("n1")]
        .prepare_native_ready(
            &initiator,
            &NativeReadyRequest {
                scope: scope.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap_err();
    assert!(
        behind_ready_error
            .to_string()
            .contains("sparse history is not fully materialized"),
        "{behind_ready_error}"
    );
    let mut checked = 0;
    for line in std::fs::read_to_string(
        fixture
            .root
            .path()
            .join("controller-acknowledged-1025.jsonl"),
    )
    .unwrap()
    .lines()
    {
        let ledger: serde_json::Value = serde_json::from_str(line).unwrap();
        let record = fixture.databases[&node("n0")]
            .read()
            .get("items", ledger["acknowledged_key"].as_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            ledger["record_sha256"],
            hex::encode(Sha256::digest(serde_json::to_vec(&record).unwrap()))
        );
        checked += 1;
    }
    assert_eq!(checked, 1025);
    // Both original sources are still byte-identical; all opens/refsyncs were
    // confined to the new diagnostic copy. No original root is engine-opened.
    assert_eq!(
        std::fs::read(source.join("n0/cluster-range-write-log.json")).unwrap(),
        original_catalog
    );
    assert_eq!(
        std::fs::read(source.join("first-recovery-refusal.json")).unwrap(),
        original_refusal
    );
    wait_for_contexts(&runtimes);
    let report = serde_json::json!({"classification":"SPARSE_CURRENT_CERTIFICATION_FINISHES_NATIVE_SOURCE_TAIL",
        "source_native_before":initial_native,"source_progress_after":source_applied.fence().progress(),
        "source_native_after":source_applied.fence().native(),"sparse_decision_sha256":sparse.checksum_sha256(),
        "behind_progress_after_sparse":sparse_observation.fence().progress(),
        "behind_native_after_sparse":sparse_observation.fence().native(),
        "behind_obligations":sparse_observation.fence().image_history(),
        "normal_tail_gap_refusal":tail_gap.to_string(),"behind_ready_error":behind_ready_error.to_string(),
        "target_image_obligations":after_image_obligations.irrevocable_commands(),
        "prior_target_image_token_invalidated_without_raw_progress_change":true,
        "source_ready_index":source_ready.range_index(),"acknowledged_values_reconciled":checked,
        "unknown_tail_is_not_new_application_ack":true,
        "original_catalog_and_refusal_bytes_unchanged":true,"physical_qualification":false,
        "behind_image_install_tested":false,"activation_proven":false,"new_application_ack_tested":false});
    std::fs::write(
        fixture
            .root
            .path()
            .join("sparse-current-certification-result.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("SPARSE_CURRENT_CERTIFICATION_RESULT {report}");
}
