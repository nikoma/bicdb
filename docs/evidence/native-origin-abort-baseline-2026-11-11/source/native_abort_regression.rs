//! Actual origin liveness after an unacknowledged failed prepare. Deliberately
//! unavailable in-process deliveries are not a host fault or TLS evidence.
use super::*;

#[test]
fn native_failed_prepare_abort_allows_next_origin_ack_and_canonical_history() {
    let mut fixture = ActualNativeRf3Fixture::new();
    fixture.root.disable_cleanup(true);
    eprintln!(
        "NATIVE_ABORT_RETAINED_ROOT {}",
        fixture.root.path().display()
    );
    let source = ClusterNodeId::new("n0").unwrap();
    let range = fixture
        .topology
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    let initial = fixture.databases[&source]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(initial.native_applied_index, Some(0));
    assert!(initial.factory_initialization_complete && !initial.invalidated);
    fixture
        .transport
        .set_unavailable(&ClusterNodeId::new("n1").unwrap(), true);
    fixture
        .transport
        .set_unavailable(&ClusterNodeId::new("n2").unwrap(), true);
    let first_error = fixture
        .append("unacknowledged-failed-prepare-1")
        .unwrap_err();
    assert!(
        first_error.to_string().contains("failed prepare quorum"),
        "{first_error}"
    );
    assert!(
        !matches!(first_error, BicDbError::CommitOutcomeUnknown { .. }),
        "prepare never reached decision quorum"
    );
    let abort = fixture.services[&source]
        .lock()
        .probe_range_write_for_peer(&range, &source, 1)
        .unwrap()
        .entry
        .unwrap();
    assert_eq!(abort.state, RangeWriteState::Aborted);
    let after_abort_raw = fixture.services[&source]
        .lock()
        .range_write_progress(range.id);
    assert_eq!(
        (after_abort_raw.last_index, after_abort_raw.resolved_through),
        (1, 1)
    );
    let after_abort_native = fixture.databases[&source]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(after_abort_native.pending_command_index, None);
    assert!(fixture.databases[&source]
        .read()
        .get("items", "unacknowledged-failed-prepare-1")
        .unwrap()
        .is_none());
    for name in ["n1", "n2"] {
        let id = ClusterNodeId::new(name).unwrap();
        assert_eq!(
            fixture.services[&id]
                .lock()
                .range_write_progress(range.id)
                .last_index,
            0
        );
        fixture.transport.set_unavailable(&id, false);
    }
    let second = fixture.append("ack-after-aborted-prefix-2");
    let after_second_native = fixture.databases[&source]
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    let after_second_raw = fixture.services[&source]
        .lock()
        .range_write_progress(range.id);
    let report = serde_json::json!({"classification":if second.is_ok(){"NATIVE_ABORT_NEXT_ACK_SUCCEEDED"}else{"NATIVE_ABORT_NEXT_ACK_REFUSED"},"transport":"actual fsynced services; in-process data delivery only","first_call_acknowledged":false,"first_error":first_error.to_string(),"actual_abort":abort,"initial_source_native":initial,"after_abort_raw":after_abort_raw,"after_abort_native":after_abort_native,"second_call_acknowledged":second.is_ok(),"second_error":second.as_ref().err().map(ToString::to_string),"after_second_raw":after_second_raw,"after_second_native":after_second_native,"no_counter_reset_or_epoch_seed":true,"physical_qualification":false,"strong_get_tested":false,"irretrievable_loss_proven":false});
    std::fs::write(
        fixture.root.path().join("native-abort-history-report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("NATIVE_ABORT_HISTORY_REPORT {report}");
    second.expect("restoring the same actual quorum after an unacknowledged prepare abort must allow the next real origin transaction");
    assert_eq!(after_second_native.native_applied_index, Some(2));
    assert!(fixture.databases[&source]
        .read()
        .get("items", "ack-after-aborted-prefix-2")
        .unwrap()
        .is_some());
    let db = fixture.databases[&source].read();
    let lineage = db.current_native_range_source_lineage(range.id).unwrap();
    let frozen = db
        .range_mutation_gate()
        .try_freeze(range.id, Uuid::now_v7())
        .unwrap();
    let actual_history = db
        .export_native_logical_history_step(
            &lineage,
            &frozen,
            0,
            16,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    assert_eq!(actual_history.next_index, None);
    assert_eq!(
        actual_history
            .entries
            .iter()
            .find(|e| e.index == 1)
            .unwrap()
            .outcome,
        crate::db::NativeLogicalHistoryOutcomeV1::Aborted
    );
}
