//! Actual full handoff/ACK2/Strong, actual CurrentJoint fenced/Prepared/Completed.
//! Scope identity/cancellation/revocation cuts use genuine objects, never DTOs.
use super::post_joint_no_data_tests::{diagnostic_cut, fresh_new_scope};
use super::*;
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_recovery::NativeReadyRequest;
use std::sync::atomic::{AtomicBool, Ordering};

#[test]
fn native_current_joint_observer_scope_actual_foreign_identity_cancel_and_hook_cuts() {
    assert_eq!(
        run_complete_joint_with_continuation(0, JointFullContinuation::ObserverScopeCuts),
        JointTestOutcome::FullHandoffThenObserverScopeCuts
    );
}
fn token() -> CancellationToken {
    CancellationToken::uncancelable().child_with_deadline(Instant::now() + Duration::from_secs(5))
}
pub(super) fn exercise(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    ack1: &crate::Record,
    ack2: &crate::Record,
) {
    let key = fixture.key(range.id, "observer-scope-no-extra-ACK");
    let inherited = [node("n0"), node("n1"), node("n2"), node("n3")]
        .into_iter()
        .map(|id| {
            let state = post_joint_no_data_tests::snapshot(fixture, range, &id);
            (id, state)
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
    // Real completed pointer/archive on n1, exercising the measured binding path.
    let complete = fixture.clients[&range.leader]
        .complete_native_recovery_observation(
            &node("n1"),
            &crate::distribution_range_recovery::NativeCompletionRequest {
                scope: scope.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 4000,
            },
            &token(),
        )
        .unwrap();
    assert_eq!(complete.reply().range_index, 2);
    assert_eq!(
        complete.reply().stage,
        crate::distribution_range_recovery::NativeCompletionStage::CompletedFrozen
    );
    fixture.idle();
    let before = diagnostic_cut(fixture, range, ack1, ack2, &key);
    let runtime = &fixture.runtimes[&node("n1")];
    let actor = fixture.clients[&range.leader].local_metadata_read_participant();
    let make_request = || NativeReadyRequest {
        scope: scope.clone(),
        challenge: Uuid::now_v7(),
        budget_ms: 4000,
    };

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
    let foreign_database = fixture.native.databases[&node("n2")].clone();
    let identities_checked = Arc::new(AtomicBool::new(false));
    let identities_cut = identities_checked.clone();
    let parent = token();
    let request = make_request();
    let reply = BicDb::test_with_native_ready_observer_scope_cut(
        move |actual| {
            let db = foreign_database
                .try_read()
                .expect("real foreign DB borrow available");
            let error = actual
                .require_identity(
                    &db,
                    actual.prepared(),
                    actual.store(),
                    actual.observer(),
                    actual.request(),
                )
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("substituted actual DB/Prepared/Store/observer/request"));
            drop(db);
            let store = foreign_store
                .try_lock()
                .expect("real foreign Store borrow available");
            let error = actual
                .require_identity(
                    actual.database(),
                    actual.prepared(),
                    &store,
                    actual.observer(),
                    actual.request(),
                )
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("substituted actual DB/Prepared/Store/observer/request"));
            identities_cut.store(true, Ordering::Release);
            Ok(())
        },
        || runtime.observe_native_ready(&actor, &request, &parent),
    )
    .unwrap();
    parent.check().unwrap();
    assert!(identities_checked.load(Ordering::Acquire));
    assert_eq!(reply.reply().range_index, 2);
    assert_eq!(diagnostic_cut(fixture, range, ack1, ack2, &key), before);

    let canceled = token();
    let cut_parent = canceled.clone();
    let canceled_entered = Arc::new(AtomicBool::new(false));
    let reached = canceled_entered.clone();
    let request = make_request();
    let error = BicDb::test_with_native_ready_observer_scope_cut(
        move |_| {
            reached.store(true, Ordering::Release);
            cut_parent.cancel();
            Ok(())
        },
        || runtime.observe_native_ready(&actor, &request, &canceled),
    )
    .unwrap_err();
    assert!(canceled_entered.load(Ordering::Acquire));
    assert!(matches!(error, crate::BicDbError::QueryCanceled), "{error}");
    fixture.idle();
    assert_eq!(diagnostic_cut(fixture, range, ack1, ack2, &key), before);
    // A NEW real TLS observation after the canceled attempt, not revived authority.
    let live = token();
    let status = fixture.clients[&range.leader]
        .observe_native_ready_observation(&node("n1"), &make_request(), &live)
        .unwrap();
    live.check().unwrap();
    assert_eq!(status.reply().range_index, 2);
    fixture.idle();

    let replacement = fixture.coordinators[&node("n2")].clone();
    let replaced = Arc::new(AtomicBool::new(false));
    let did_replace = replaced.clone();
    let parent = token();
    let request = make_request();
    let error = BicDb::test_with_native_ready_observer_scope_cut(
        move |actual| {
            // REAL public hook replacement revokes the original registration without
            // recursively publishing a native enrollment under our retained journal.
            actual
                .database()
                .install_commit_admission(replacement.clone());
            did_replace.store(true, Ordering::Release);
            Ok(())
        },
        || runtime.observe_native_ready(&actor, &request, &parent),
    )
    .unwrap_err();
    parent.check().unwrap();
    assert!(replaced.load(Ordering::Acquire));
    assert!(
        error
            .to_string()
            .contains("native writer hook lifetime ended")
            || error
                .to_string()
                .contains("native writer hook/open is no longer current")
            || error.to_string().contains("actual installed hook"),
        "{error}"
    );
    fixture.idle();
    assert_eq!(
        diagnostic_cut(fixture, range, ack1, ack2, &key),
        before,
        "hook revocation cannot change native/raw/history/UID/B/rows/ACKs"
    );
    for id in [node("n1"), node("n2"), node("n3")] {
        let frozen = fixture.runtimes[&id].source_capture_freeze(&scope).unwrap();
        fixture.native.databases[&id]
            .read()
            .range_mutation_gate()
            .validate_quiescent_frozen(range.id, &frozen)
            .unwrap();
    }
    assert!(fixture.hosts[&node("n1")]
        .require_for_coordinator(
            &fixture.native.databases[&node("n1")].read(),
            range,
            &fixture.coordinators[&node("n1")]
        )
        .is_err());
}
