//! Manual external-artifact case. The preserved root is READ/COPIED only;
//! native and metadata engines open a new owned TempDir, never the original.
use super::*;

#[test]
#[ignore = "requires explicit BICDB_RETAINED_NATIVE_RECOVERY_ROOT; copies retained actual 1025-ACK fixture"]
fn copied_actual_1025_prefix_fences_complete_obligations_then_reveals_compacted_gap() {
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
        "COPIED_OBLIGATION_RETAINED_ROOT {}",
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
    let frozen = runtimes[&node("n0")].source_capture_freeze(&scope).unwrap();
    let role = fresh_role(&providers, &initiator, &range, scope.recovery_nonce);
    let ready_error = fixture.services[&node("n0")]
        .lock()
        .attest_native_recovery_ready(role, frozen, &parent)
        .unwrap_err();
    assert!(
        ready_error
            .to_string()
            .contains("historical prefix/tail is not fully resolved"),
        "{ready_error}"
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
    let report = serde_json::json!({"classification":"COMPLETE_FENCE_OBLIGATIONS_THEN_FAIL_CLOSED_COMPACTED_GAPS","source_native":initial_native,"actual_source_obligations":history,"prefix_export_error":prefix_error.to_string(),"direct_tail_error":tail_gap.to_string(),"source_ready_error":ready_error.to_string(),"acknowledged_values_reconciled":checked,"original_catalog_and_refusal_bytes_unchanged":true,"source_scope_is_untrusted_request":true,"actual_fresh_metadata_and_same_session_refsync":true,"physical_qualification":false,"activation_proven":false,"new_application_ack_tested":false,"irretrievable_loss_proven":false});
    std::fs::write(
        fixture.root.path().join("copied-obligation-result.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("COPIED_OBLIGATION_RESULT {report}");
}
