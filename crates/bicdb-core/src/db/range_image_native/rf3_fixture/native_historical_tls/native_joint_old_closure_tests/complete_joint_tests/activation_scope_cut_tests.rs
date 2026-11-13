//! Real all3 handoff/release + ACK2/Strong, then actual CurrentJoint
//! Completed R2 and durable catalogue activation. No synthetic role/receipt.
use super::post_joint_no_data_tests::{diagnostic_cut, fresh_new_scope};
use super::*;
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_recovery::{NativeCompletionRequest, NativeReadyRequest};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ActivationScopeCut {
    ForeignIdentity,
    OriginalCallerCancel,
    WriterReplacement,
    MissingDecisionArchive,
}
fn run(cut: ActivationScopeCut) {
    assert_eq!(
        run_complete_joint_with_continuation(0, JointFullContinuation::ActivationScopeCut(cut)),
        JointTestOutcome::FullHandoffThenActivationScopeCut
    );
}
#[test]
fn native_current_joint_activation_scope_actual_postcat_foreign_db_and_store_refused() {
    run(ActivationScopeCut::ForeignIdentity);
}
#[test]
fn native_current_joint_activation_scope_actual_postcat_caller_cancel_and_exact_fresh_recovery() {
    run(ActivationScopeCut::OriginalCallerCancel);
}
#[test]
fn native_current_joint_activation_scope_actual_postcat_writer_replacement_refused() {
    run(ActivationScopeCut::WriterReplacement);
}
#[test]
fn native_current_joint_activation_scope_actual_postcat_missing_decision_archive_refused() {
    run(ActivationScopeCut::MissingDecisionArchive);
}
fn token() -> CancellationToken {
    CancellationToken::uncancelable().child_with_deadline(Instant::now() + Duration::from_secs(5))
}
fn unchanged_data(mut value: serde_json::Value) -> serde_json::Value {
    // The exact durable Activated phase is asserted separately. These are
    // control-plane changes, never additional business ACKs or native R/B.
    for key in ["catalogue_sha256", "recovery_fence", "progress"] {
        value.as_object_mut().unwrap().remove(key);
    }
    value
}
fn full_rows(fixture: &EnforcedNetwork) -> BTreeMap<ClusterNodeId, Vec<crate::Record>> {
    fixture
        .native
        .databases
        .iter()
        .map(|(id, database)| {
            let db = database.read();
            let mut rows = db.scan_collection("items").unwrap();
            rows.sort_by(|a, b| a.id.cmp(&b.id));
            (id.clone(), rows)
        })
        .collect()
}
pub(super) fn exercise(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    ack1: &crate::Record,
    ack2: &crate::Record,
    cut: ActivationScopeCut,
) {
    let key = fixture.key(range.id, "activation-scope-no-extra-business-ACK");
    let inherited = [node("n0"), node("n1"), node("n2"), node("n3")]
        .into_iter()
        .map(|id| {
            (
                id.clone(),
                post_joint_no_data_tests::snapshot(fixture, range, &id),
            )
        })
        .collect();
    let scope = fresh_new_scope(fixture, range, &inherited, ack1, ack2, &key);
    for id in [node("n1"), node("n2"), node("n3")] {
        fixture.clients[&range.leader]
            .prepare_native_ready_observation(
                &id,
                &NativeReadyRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4000,
                },
                &token(),
            )
            .unwrap();
        fixture.idle();
    }
    for id in [node("n1"), node("n2"), node("n3")] {
        let completed = fixture.clients[&range.leader]
            .complete_native_recovery_observation(
                &id,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4000,
                },
                &token(),
            )
            .unwrap();
        assert_eq!(completed.reply().range_index, 2);
        assert_eq!(
            completed.reply().stage,
            crate::distribution_range_recovery::NativeCompletionStage::CompletedFrozen
        );
        fixture.idle();
    }
    let before = diagnostic_cut(fixture, range, ack1, ack2, &key);
    let rows_before = full_rows(fixture);
    let frozen_identities = [node("n1"), node("n2"), node("n3")]
        .into_iter()
        .map(|id| {
            let frozen = fixture.runtimes[&id].source_capture_freeze(&scope).unwrap();
            (id, Arc::as_ptr(&frozen) as usize)
        })
        .collect::<BTreeMap<_, _>>();
    let foreign_root = tempfile::tempdir().unwrap();
    let foreign_store = Arc::new(Mutex::new(
        RangeWriteStore::open(
            foreign_root.path(),
            scope.cluster_id.clone(),
            node("n2"),
            true,
        )
        .unwrap(),
    ));
    let foreign_db = fixture.native.databases[&node("n2")].clone();
    let replacement = fixture.coordinators[&node("n2")].clone();
    let parent = token();
    let canceled = parent.clone();
    let reached = Arc::new(AtomicBool::new(false));
    let actual_cut = reached.clone();
    let actor = fixture.clients[&range.leader].local_metadata_read_participant();
    let request = NativeCompletionRequest {
        scope: scope.clone(),
        challenge: Uuid::now_v7(),
        budget_ms: 4000,
    };
    let error = BicDb::test_with_native_activation_post_catalogue_cut(
        move |actual| {
            actual.parent().check()?;
            actual_cut.store(true, Ordering::Release);
            match cut {
                ActivationScopeCut::ForeignIdentity => {
                    let db = foreign_db
                        .try_read()
                        .expect("actual foreign DB borrow available");
                    let mismatch = actual
                        .require_identity(&db, actual.prepared(), actual.store(), actual.role())
                        .unwrap_err();
                    assert!(mismatch
                        .to_string()
                        .contains("substituted actual DB/Prepared/Store/role"));
                    drop(db);
                    let store = foreign_store
                        .try_lock()
                        .expect("actual foreign Store borrow available");
                    let mismatch = actual
                        .require_identity(
                            actual.database(),
                            actual.prepared(),
                            &store,
                            actual.role(),
                        )
                        .unwrap_err();
                    assert!(mismatch
                        .to_string()
                        .contains("substituted actual DB/Prepared/Store/role"));
                    return Err(mismatch);
                }
                ActivationScopeCut::OriginalCallerCancel => canceled.cancel(),
                ActivationScopeCut::WriterReplacement => actual
                    .database()
                    .install_commit_admission(replacement.clone()),
                ActivationScopeCut::MissingDecisionArchive => actual
                    .database()
                    .test_remove_native_activation_archive_in_scope(actual)?,
            }
            Ok(())
        },
        || fixture.runtimes[&node("n1")].activate_native_recovery(&actor, &request, &parent),
    )
    .unwrap_err();
    assert!(
        reached.load(Ordering::Acquire),
        "must reach ACTUAL durable postCAT scope, not an earlier timeout: {error}"
    );
    match cut {
        ActivationScopeCut::OriginalCallerCancel => {
            assert!(matches!(error, crate::BicDbError::QueryCanceled), "{error}")
        }
        ActivationScopeCut::ForeignIdentity => {
            parent.check().unwrap();
            assert!(
                error
                    .to_string()
                    .contains("substituted actual DB/Prepared/Store/role"),
                "{error}"
            );
        }
        ActivationScopeCut::WriterReplacement => {
            parent.check().unwrap();
            assert!(
                error.to_string().contains("native writer hook")
                    || error.to_string().contains("actual installed hook"),
                "{error}"
            );
        }
        ActivationScopeCut::MissingDecisionArchive => {
            parent.check().unwrap();
            assert!(
                error
                    .to_string()
                    .contains("immutable protected activation decision is missing"),
                "{error}"
            );
        }
    }
    fixture.idle();
    let after = diagnostic_cut(fixture, range, ack1, ack2, &key);
    assert_eq!(full_rows(fixture), rows_before);
    assert!(
        after[&node("n1")]["recovery_fence"]["phase"]
            .get("NativeActivated")
            .is_some(),
        "actual catalogue must already be Activated, not an earlier denial: {}",
        after[&node("n1")]
    );
    for (id, original) in &before {
        assert_eq!(
            unchanged_data(after[id].clone()),
            unchanged_data(original.clone()),
            "{id}: no extra ACK/native R/H/B/UID/slot/promise/row changes"
        );
    }
    for id in [node("n1"), node("n2"), node("n3")] {
        let frozen = fixture.runtimes[&id].source_capture_freeze(&scope).unwrap();
        assert_eq!(
            Arc::as_ptr(&frozen) as usize,
            frozen_identities[&id],
            "original Frozen owner must remain identical"
        );
        fixture.native.databases[&id]
            .read()
            .range_mutation_gate()
            .validate_quiescent_frozen(range.id, &frozen)
            .unwrap();
    }
    if cut == ActivationScopeCut::OriginalCallerCancel {
        let actual_catalogue = after[&node("n1")]["recovery_fence"].clone();
        let next = token();
        let result = fixture.runtimes[&node("n1")]
            .activate_native_recovery(
                &actor,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4000,
                },
                &next,
            )
            .unwrap();
        next.check().unwrap();
        assert_eq!(result.reply().range_index, 2);
        assert_eq!(
            result.reply().stage,
            crate::distribution_range_recovery::NativeCompletionStage::ActivatedAnchor
        );
        fixture.idle();
        let recovered = diagnostic_cut(fixture, range, ack1, ack2, &key);
        assert_eq!(full_rows(fixture), rows_before);
        assert_eq!(
            recovered[&node("n1")]["recovery_fence"],
            actual_catalogue,
            "distinct fresh authority must preserve exact original durable activation audit"
        );
        for (id, original) in &before {
            assert_eq!(
                unchanged_data(recovered[id].clone()),
                unchanged_data(original.clone())
            );
        }
        assert!(
            parent.check().is_err(),
            "fresh recovery cannot revive original caller"
        );
    }
    eprintln!("ACTUAL_ACTIVATION_SCOPE_POSTCAT_CUT reached=true nativeR=2 no-extra-businessACK=true failed-cut-retained-actualFrozen=true");
}
