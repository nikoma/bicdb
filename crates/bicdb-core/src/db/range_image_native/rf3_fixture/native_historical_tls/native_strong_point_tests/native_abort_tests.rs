//! Genuine from-birth writer enforcement and actual TLS permanent promises.
//! No raw Aborted/empty transaction/row absence is used to mint a no-data proof.
use super::*;
use crate::distribution_range_consensus::RangeWriteTransport;
use crate::distribution_range_recovery::{
    NativeAbortRequest, NativeCompletionRequest, NativeReadyRequest,
};

fn token() -> CancellationToken {
    CancellationToken::uncancelable().child_with_deadline(Instant::now() + Duration::from_secs(5))
}
#[test]
fn native_abort_actual_enforced_prepare_failure_canonical_metadata_history_then_next_ack() {
    let fixture = EnforcedNetwork::new();
    let range = fixture.ranges()[0].clone();
    let failed_key = fixture.key(range.id, "definitely-unacked-prepare-only");
    let before_b = [node("n0"), node("n1"), node("n2")]
        .into_iter()
        .map(|id| {
            let b = fixture.native.databases[&id]
                .read()
                .current_native_range_source_lineage(range.id)
                .unwrap()
                .actual_local_visibility_sequence();
            (id, b)
        })
        .collect::<BTreeMap<_, _>>();
    for id in [node("n1"), node("n2")] {
        fixture.native.services[&id]
            .lock()
            .test_refuse_native_prepare_after_start(true);
    }
    let failed = fixture.native.append(&failed_key).unwrap_err();
    assert!(
        !matches!(&failed, BicDbError::CommitOutcomeUnknown { .. }),
        "prepare-only cleanup did not produce a definite no-effect result: {failed}"
    );
    // No ACK is recorded for this attempt. The source remains Prepared, not a
    // guessed global Aborted. Native R/dataB have not advanced.
    let original = fixture.native.services[&node("n0")]
        .lock()
        .prepared_range_writes(range.id)
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(original.index, 1);
    for id in [node("n0"), node("n1"), node("n2")] {
        let db = fixture.native.databases[&id].read();
        assert!(db.get("items", &failed_key).unwrap().is_none());
        assert_eq!(
            db.current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            0
        );
        if id != node("n0") {
            fixture.native.services[&id]
                .lock()
                .test_refuse_native_prepare_after_start(false);
        }
    }
    let scope = fixture.actual_current_recovery_scope(&range);
    let request = NativeAbortRequest {
        scope: scope.clone(),
        range_index: 1,
        origin_node_id: node("n0"),
        candidate_sha256: None,
        challenge: Uuid::now_v7(),
        budget_ms: 4500,
    };
    let actual_candidate = fixture.clients[&node("n0")]
        .observe_native_abort_candidate(&node("n0"), &request, &token())
        .unwrap();
    assert_eq!(actual_candidate.record().command, original);
    assert_eq!(actual_candidate.record().predecessor_index(), 0);
    let request = NativeAbortRequest {
        candidate_sha256: Some(actual_candidate.sha256().into()),
        ..request
    };
    for id in [node("n0"), node("n1"), node("n2")] {
        let (r, decision, b) = fixture.clients[&node("n0")]
            .resolve_native_abort_at(&id, &request, &token())
            .unwrap();
        assert_eq!(r, 1);
        assert_eq!(decision.len(), 64);
        assert_eq!(b, before_b[&id]);
        fixture.idle();
        let db = fixture.native.databases[&id].read();
        let source = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(source.range_index(), 1);
        assert_eq!(source.actual_local_visibility_sequence(), before_b[&id]);
        assert_eq!(
            db.read_native_history_entry(range.id, source.history_generation(), 1)
                .unwrap()
                .outcome,
            crate::db::range_image_native::history::NativeLogicalHistoryOutcomeV1::Aborted
        );
        assert!(db.get("items", &failed_key).unwrap().is_none());
        assert_eq!(
            fixture.native.services[&id]
                .lock()
                .range_write_progress(range.id)
                .resolved_through,
            1
        );
    }
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&node("n0")]
            .prepare_native_ready_observation(
                &id,
                &NativeReadyRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4500,
                },
                &token(),
            )
            .unwrap();
        fixture.idle();
    }
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&node("n0")]
            .complete_native_recovery_observation(
                &id,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4500,
                },
                &token(),
            )
            .unwrap();
        fixture.idle();
    }
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    for id in [node("n1"), node("n2"), node("n0")] {
        fixture.runtimes[&id]
            .activate_native_recovery(
                &actor,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4500,
                },
                &token(),
            )
            .unwrap();
        fixture.idle();
    }
    fixture.coordinators[&node("n0")]
        .recover_local_leadership(8)
        .unwrap();
    let acked_key = fixture.key(range.id, "actual-next-ack-after-canonical-abort");
    fixture.native.append(&acked_key).unwrap();
    for id in [node("n0"), node("n1"), node("n2")] {
        let db = fixture.native.databases[&id].read();
        assert!(db.get("items", &failed_key).unwrap().is_none());
        assert!(db.get("items", &acked_key).unwrap().is_some());
        let source = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(source.range_index(), 2);
        assert_eq!(
            db.read_native_history_entry(range.id, source.history_generation(), 1)
                .unwrap()
                .outcome,
            crate::db::range_image_native::history::NativeLogicalHistoryOutcomeV1::Aborted
        );
    }
}
