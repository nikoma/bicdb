//! The original INITIAL R2 controls have no serving record. These controls
//! first run the existing real NormalRecovery producer, not a JSON constructor.
use super::*;
use crate::distribution_range_recovery::{NativeCompletionRequest, NativeReadyRequest};

fn normal_recovered() -> (EnforcedNetwork, RangeDescriptor) {
    let mut fixture = EnforcedNetwork::new();
    let (ack, old_range) = fixture.seed();
    let original = fixture.native.databases[&node("n0")]
        .read()
        .get("items", &ack)
        .unwrap()
        .unwrap();
    let path = fixture
        .native
        .root
        .path()
        .join("actual-joint-selected-range-ack-ledger.json");
    std::fs::write(&path, serde_json::to_vec(&[original]).unwrap()).unwrap();
    std::fs::File::open(&path).unwrap().sync_all().unwrap();
    std::fs::File::open(fixture.native.root.path())
        .unwrap()
        .sync_all()
        .unwrap();
    // Same actual metadata+policy transition and existing purpose producer as
    // after_transfer.rs; no direct recovered-epoch/Serving/freeze assignments.
    fixture.change_committed_leader(old_range.id);
    let range = fixture
        .native
        .topology
        .read()
        .range_by_id(old_range.id)
        .unwrap()
        .clone();
    assert_eq!(range.leader, node("n2"));
    assert_eq!(range.epoch, old_range.epoch + 1);
    fixture.refresh_actual_writer_policies();
    let scope = fixture.actual_current_recovery_scope(&range);
    assert!(matches!(
        &scope.purpose,
        MetadataRecoveryRolePurpose::NormalRecovery
    ));
    for id in [node("n0"), node("n1"), node("n2")] {
        let ready = fixture.clients[&range.leader]
            .prepare_native_ready_observation(
                &id,
                &NativeReadyRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        assert_eq!(ready.reply().range_index, 1);
        fixture.idle();
    }
    for id in [node("n0"), node("n1"), node("n2")] {
        let completed = fixture.clients[&range.leader]
            .complete_native_recovery_observation(
                &id,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        assert_eq!(completed.reply().range_index, 1);
        fixture.idle();
    }
    let actor = fixture.clients[&range.leader].local_metadata_read_participant();
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.runtimes[&id]
            .activate_native_recovery(
                &actor,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        fixture.idle();
    }
    fixture.coordinators[&range.leader]
        .recover_local_leadership(8)
        .unwrap();
    (fixture, range)
}
#[derive(Clone, Copy)]
enum Fault {
    Current,
    Archive,
}
fn fault(fault: Fault) {
    let (fixture, range) = normal_recovered();
    let before = snapshot_at(&fixture, &range, 1);
    let parent = token();
    let business = Uuid::now_v7();
    let metadata = fixture.clients[&range.leader]
        .acquire_metadata_read_observation(
            &node("n1"),
            business,
            range.id,
            range.epoch,
            Duration::from_secs(5),
            &parent,
        )
        .unwrap();
    let original_metadata_deadline = metadata.deadline();
    let actual_round =
        NativeReadQuorumRound::begin(metadata, business, Uuid::now_v7(), &range.leader, &parent)
            .unwrap();
    let request = actual_round.request().clone();
    let reached = Arc::new(AtomicBool::new(false));
    let actual_reached = reached.clone();
    let service = fixture.native.services[&node("n0")].lock();
    let error = BicDb::test_with_native_serving_read_scope_cut(
        move |actual| {
            if matches!(fault, Fault::Archive) {
                actual
                    .database()
                    .test_remove_serving_read_normal_archive(actual)?;
            } else {
                actual
                    .database()
                    .test_remove_serving_read_normal_current(actual)?;
            }
            // The fault helper proves ACTUAL NormalRecovery current==archive
            // before its one native WAL deletion and proves the other survived.
            actual_reached.store(true, Ordering::Release);
            Ok(())
        },
        || service.observe_current_native_range(&request, &parent),
    )
    .unwrap_err();
    drop(service);
    assert!(
        reached.load(Ordering::Acquire),
        "genuine normal-serving scope/cut not reached: {error}"
    );
    parent.check().unwrap();
    assert!(
        Instant::now() < original_metadata_deadline,
        "original actual MQ expired before the serving-record refusal"
    );
    let expected = if matches!(fault, Fault::Archive) {
        "serving read immutable serving archive missing"
    } else {
        "serving read actual current serving pointer changed"
    };
    assert!(
        error.to_string().contains(expected),
        "wrong refusal instead of actual after-entry record damage: {error}"
    );
    fixture.idle();
    // Only the deliberately selected serving metadata record ended. The
    // actual immutable data head, local B, UID, full rows/raw slots/ACK stay.
    assert_eq!(snapshot_at(&fixture, &range, 1), before);
}
#[test]
fn native_serving_observation_scope_actual_normal_serving_current_damage_refused_at_exit() {
    fault(Fault::Current);
}
#[test]
fn native_serving_observation_scope_actual_normal_serving_archive_damage_refused_at_exit() {
    fault(Fault::Archive);
}
