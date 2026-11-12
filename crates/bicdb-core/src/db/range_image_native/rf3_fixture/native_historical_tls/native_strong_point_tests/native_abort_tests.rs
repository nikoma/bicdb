//! Genuine from-birth writer enforcement and actual TLS permanent promises.
//! No raw Aborted/empty transaction/row absence is used to mint a no-data proof.
use super::*;
use crate::distribution_range_consensus::RangeWriteTransport;
use crate::distribution_range_recovery::{
    NativeAbortRequest, NativeCompletionRequest, NativeReadyRequest,
};

pub(super) fn token() -> CancellationToken {
    CancellationToken::uncancelable().child_with_deadline(Instant::now() + Duration::from_secs(5))
}
#[test]
fn native_abort_actual_enforced_prepare_failure_canonical_metadata_history_then_next_ack() {
    actual_prepare_failure_lifecycle(false);
}
#[test]
fn native_abort_actual_enforced_prior_leader_no_effect_fresh_e2_abort_then_new_leader_ack() {
    actual_prepare_failure_lifecycle(true);
}
fn actual_prepare_failure_lifecycle(transfer_before_abort: bool) {
    let mut fixture = EnforcedNetwork::new();
    let mut range = fixture.ranges()[0].clone();
    let prior_ack = if transfer_before_abort {
        // Four genuine ACKs on the OTHER range make actual local dataB
        // deliberately different from the selected logical range index.
        let other = fixture
            .ranges()
            .into_iter()
            .find(|r| r.id != range.id)
            .unwrap();
        for n in 0..4 {
            fixture
                .native
                .append(&fixture.key(other.id, &format!("other-range-real-ack-{n}")))
                .unwrap();
        }
        let key = fixture.key(range.id, "prior-real-ack-survives-no-data-e2");
        let db = fixture.native.databases[&node("n0")].write();
        let mut tx = db.begin_transaction().unwrap();
        let mut row = Record::new(&key);
        row.metadata = serde_json::json!({"ack_ledger":"before-no-data", "must_survive":17});
        tx.insert("items", row).unwrap();
        tx.commit().unwrap();
        Some(key)
    } else {
        None
    };
    let predecessor_r = u64::from(prior_ack.is_some());
    let abort_index = predecessor_r + 1;
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
    let database = fixture.native.databases[&node("n0")].write();
    let app_wal_before = database.tx_log_handle().stats();
    let mut tx = database.begin_transaction().unwrap();
    tx.insert("items", Record::new(&failed_key)).unwrap();
    let observer = tx.observe_outcome();
    let failed = tx.commit().unwrap_err();
    assert_eq!(observer.outcome(), crate::TransactionOutcome::Aborted);
    assert_eq!(
        database.tx_log_handle().stats().written_seq,
        app_wal_before.written_seq
    );
    assert_eq!(
        database.tx_log_handle().stats().commits_written,
        app_wal_before.commits_written
    );
    drop(database);
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
    assert_eq!(original.index, abort_index);
    for id in [node("n0"), node("n1"), node("n2")] {
        let db = fixture.native.databases[&id].read();
        assert!(db.get("items", &failed_key).unwrap().is_none());
        assert_eq!(
            db.current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            predecessor_r
        );
        if id != node("n0") {
            fixture.native.services[&id]
                .lock()
                .test_refuse_native_prepare_after_start(false);
        }
    }
    // A peer that refused before RawPrepare still has an actual protected
    // writer-start. Raw R0 alone must not mint a healthy empty observation.
    let topology = fixture.native.topology.read().clone();
    let read_request = crate::distribution_native_read::NativeRangeReadRequest {
        protocol_version: crate::distribution_native_read::NATIVE_RANGE_READ_VERSION,
        scope: crate::distribution_native_read::NativeRangeReadScope {
            cluster_id: topology.cluster_id.clone(),
            bootstrap_id: topology.range_image_bootstrap_id.unwrap(),
            range: range.clone(),
            members: topology
                .nodes
                .iter()
                .map(|(id, n)| {
                    (
                        id.clone(),
                        crate::distribution_range_recovery::RecoveryMemberIdentity {
                            incarnation: n.incarnation,
                            active_certificate_sha256: n.tls_certificate_sha256.clone(),
                            pending_certificate_sha256: n.pending_tls_certificate_sha256.clone(),
                        },
                    )
                })
                .collect(),
            schema_sha256: fixture.native.databases[&node("n1")]
                .read()
                .schema_compatibility_fingerprint()
                .unwrap()
                .sha256,
            restore_activation: None,
        },
        challenge: Uuid::now_v7(),
        budget_ms: 4500,
    };
    assert_eq!(
        fixture.native.services[&node("n1")]
            .lock()
            .range_write_progress(range.id)
            .last_index,
        predecessor_r
    );
    let protected = fixture.native.services[&node("n1")]
        .lock()
        .observe_current_native_range(&read_request, &token())
        .unwrap_err();
    assert!(
        protected
            .to_string()
            .contains("unresolved protected writer-start"),
        "{protected}"
    );
    if transfer_before_abort {
        fixture.change_committed_leader(range.id);
        fixture.refresh_actual_writer_policies();
        range = fixture
            .native
            .topology
            .read()
            .range_by_id(range.id)
            .unwrap()
            .clone();
        assert_eq!(range.epoch, 2);
        assert_eq!(range.leader, node("n2"));
    }
    let leader = range.leader.clone();
    let scope = fixture.actual_current_recovery_scope(&range);
    let request = NativeAbortRequest {
        scope: scope.clone(),
        range_index: abort_index,
        origin_node_id: node("n0"),
        candidate_sha256: None,
        challenge: Uuid::now_v7(),
        budget_ms: 4500,
    };
    let actual_candidate = fixture.clients[&leader]
        .observe_native_abort_candidate(&node("n0"), &request, &token())
        .unwrap();
    assert_eq!(actual_candidate.record().command, original);
    assert_eq!(actual_candidate.record().predecessor_index(), predecessor_r);
    let request = NativeAbortRequest {
        candidate_sha256: Some(actual_candidate.sha256().into()),
        ..request
    };
    // A readonly query cannot create a missing vote, and a follower cannot
    // create it by claiming the leader's scope. Both are genuine TLS calls.
    let follower = [node("n0"), node("n1"), node("n2")]
        .into_iter()
        .find(|id| id != &leader)
        .unwrap();
    assert!(fixture.clients[&follower]
        .observe_native_abort_promise(&node("n1"), &request, &token())
        .is_err());
    assert!(fixture.clients[&follower]
        .promise_native_abort(&node("n1"), &request, &token())
        .is_err());
    for id in [node("n0"), node("n1"), node("n2")] {
        assert!(fixture.native.databases[&id]
            .read()
            .observe_actual_native_abort_promise(range.id, abort_index)
            .unwrap()
            .is_none());
    }
    // The actual leader first creates the permanent promises. Followers
    // independently collect fresh readonly observations of those same votes;
    // no follower may issue a new Promise under the leader's identity.
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&leader]
            .promise_native_abort(&id, &request, &token())
            .unwrap();
        fixture.idle();
    }
    let mut original_promises = BTreeMap::new();
    for id in std::iter::once(leader.clone()).chain(
        [node("n0"), node("n1"), node("n2")]
            .into_iter()
            .filter(|id| id != &leader),
    ) {
        let (r, decision, b) = fixture.clients[&leader]
            .resolve_native_abort_at(&id, &request, &token())
            .unwrap();
        assert_eq!(r, abort_index);
        assert_eq!(decision.len(), 64);
        assert_eq!(b, before_b[&id]);
        if id == leader {
            fixture.idle();
            for member in [node("n0"), node("n1"), node("n2")] {
                original_promises.insert(
                    member.clone(),
                    fixture.native.databases[&member]
                        .read()
                        .observe_actual_native_abort_promise(range.id, abort_index)
                        .unwrap()
                        .unwrap(),
                );
            }
        }
        if transfer_before_abort {
            assert!(
                b > r,
                "actual interleaved dataB must not be replaced with rangeR"
            );
        }
        fixture.idle();
        for (member, original) in &original_promises {
            assert_eq!(
                fixture.native.databases[member]
                    .read()
                    .observe_actual_native_abort_promise(range.id, abort_index)
                    .unwrap()
                    .as_ref(),
                Some(original),
                "readonly collection must retain the original permanent scope/audit"
            );
        }
        let db = fixture.native.databases[&id].read();
        let source = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(source.range_index(), abort_index);
        assert_eq!(source.actual_local_visibility_sequence(), before_b[&id]);
        assert_eq!(
            db.read_native_history_entry(range.id, source.history_generation(), abort_index)
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
            abort_index
        );
    }
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&leader]
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
        fixture.clients[&leader]
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
    let actor = fixture.clients[&leader].local_metadata_read_participant();
    for id in [node("n0"), node("n1"), node("n2")]
        .into_iter()
        .filter(|id| id != &leader)
        .chain(std::iter::once(leader.clone()))
    {
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
    fixture.coordinators[&leader]
        .recover_local_leadership(8)
        .unwrap();
    let acked_key = fixture.key(range.id, "actual-next-ack-after-canonical-abort");
    fixture.native.append(&acked_key).unwrap();
    for id in [node("n0"), node("n1"), node("n2")] {
        let db = fixture.native.databases[&id].read();
        assert!(db.get("items", &failed_key).unwrap().is_none());
        assert!(db.get("items", &acked_key).unwrap().is_some());
        if let Some(prior) = &prior_ack {
            assert_eq!(
                db.get("items", prior).unwrap().unwrap().metadata,
                serde_json::json!({"ack_ledger":"before-no-data", "must_survive":17})
            );
        }
        let source = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(source.range_index(), abort_index + 1);
        assert_eq!(
            db.read_native_history_entry(range.id, source.history_generation(), abort_index)
                .unwrap()
                .outcome,
            crate::db::range_image_native::history::NativeLogicalHistoryOutcomeV1::Aborted
        );
    }
}

#[test]
fn native_abort_actual_enforced_post_durable_commit_errors_cannot_become_no_effect_or_abort() {
    let fixture = EnforcedNetwork::new();
    let range = fixture.ranges()[0].clone();
    let key = fixture.key(range.id, "unacked-durable-commit-not-abort");
    // The real services persist Commit and then return the injected publication
    // error. These are lost successful votes, not fabricated positive replies.
    for id in [node("n1"), node("n2")] {
        fixture.native.services[&id]
            .lock()
            .test_fail_native_commit_after_publication();
    }
    let db = fixture.native.databases[&node("n0")].write();
    let before = db.tx_log_handle().stats();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("items", Record::new(&key)).unwrap();
    let observer = tx.observe_outcome();
    // Retain the actual buffered transaction after the error: Drop alone
    // cannot be the cleanup boundary observed by a recovery freeze.
    let failed = db.commit_buffered_transaction(&mut tx).unwrap_err();
    assert_eq!(tx.state(), crate::TxState::Aborted);
    assert!(!tx.has_pending_writes("items"));
    assert!(
        matches!(&failed, BicDbError::CommitOutcomeUnknown { sequence: None, transaction_id: Some(id), .. }
        if *id == observer.transaction_id().0),
        "{failed}"
    );
    assert_eq!(
        observer.outcome(),
        crate::TransactionOutcome::RecoveryRequiredBeforeLocalCommit
    );
    assert_eq!(db.tx_log_handle().stats().written_seq, before.written_seq);
    assert_eq!(
        db.tx_log_handle().stats().commits_written,
        before.commits_written
    );
    // Real row-lock reuse succeeds while the failed buffered transaction is
    // STILL alive. This new local probe is rolled back without any admission.
    let mut probe = db.begin_transaction().unwrap();
    probe.insert("items", Record::new(&key)).unwrap();
    probe.rollback().unwrap();
    let gate = db.range_mutation_gate();
    let drained = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
    drop(drained);
    assert_eq!(
        observer.outcome(),
        crate::TransactionOutcome::RecoveryRequiredBeforeLocalCommit
    );
    drop(tx);
    drop(db);
    for id in [node("n0"), node("n1"), node("n2")] {
        let db = fixture.native.databases[&id].read();
        assert!(db.get("items", &key).unwrap().is_none());
        let slot = db
            .native_abort_slot_inventory()
            .unwrap()
            .into_iter()
            .find(|s| s.command.range_id == range.id && s.command.index == 1)
            .unwrap();
        assert!(slot.ever_commit);
        assert!(slot.no_effect_sha256.is_none());
        assert!(slot.abort_promise_sha256.is_none());
        assert_eq!(
            db.current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            0
        );
        // Inspect genuine persisted JSON even when a returned write error has
        // conservatively made the in-memory catalogue uncertain.
        let bytes = std::fs::read(
            db.data_path()
                .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG),
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let raw = &value["catalog"]["ranges"][range.id.get().to_string()]["entries"]["1"];
        assert_eq!(raw["state"], serde_json::json!("committed"), "{id}: {raw}");
    }
    let original = fixture.native.services[&node("n0")]
        .lock()
        .prepared_range_writes(range.id)
        .into_iter()
        .next()
        .unwrap();
    // A delayed old leader's bare Abort cannot turn a durable Commit into
    // canonical absence, even before any new recovery fence is installed.
    assert!(fixture.clients[&node("n0")]
        .abort_range_write(&node("n0"), &original)
        .is_err());
    let retry = fixture
        .native
        .append(&fixture.key(range.id, "unknown-must-recover-first"))
        .unwrap_err();
    assert!(!matches!(retry, BicDbError::CommitOutcomeUnknown { .. }));
    assert_eq!(
        observer.outcome(),
        crate::TransactionOutcome::RecoveryRequiredBeforeLocalCommit
    );
    // Re-fsync actual Commit refusals through a real fresh epoch fence, then
    // exercise the real incoming resolver. It cannot derive CoreNoEffect from
    // missing rows or from an unknown user outcome.
    let scope = fixture.actual_current_recovery_scope(&range);
    let forbidden = fixture.clients[&node("n0")]
        .resolve_native_abort_at(
            &node("n0"),
            &NativeAbortRequest {
                scope,
                range_index: 1,
                origin_node_id: node("n0"),
                candidate_sha256: None,
                challenge: Uuid::now_v7(),
                budget_ms: 4500,
            },
            &token(),
        )
        .unwrap_err();
    assert!(
        forbidden
            .to_string()
            .contains("actual Core no-effect candidate unavailable"),
        "{forbidden}"
    );
    fixture.idle();
    for id in [node("n0"), node("n1"), node("n2")] {
        let db = fixture.native.databases[&id].read();
        let slot = db
            .native_abort_slot_inventory()
            .unwrap()
            .into_iter()
            .find(|s| s.command.range_id == range.id && s.command.index == 1)
            .unwrap();
        assert!(slot.ever_commit);
        assert!(slot.no_effect_sha256.is_none());
        assert!(slot.abort_promise_sha256.is_none());
        assert_eq!(
            db.current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            0
        );
    }
}
