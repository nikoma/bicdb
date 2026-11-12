//! Four actual metadata birth identities with an exact RF3 ordinary domain.
//! Completion uses real authenticated peers; activation is the local Runtime
//! producer. This does not exercise joint membership or a handoff decision.
use super::*;
use crate::distribution_range_recovery::{
    NativeCompletionRequest, NativeCompletionStage, NativeReadyRequest,
};

#[test]
fn native_activation_four_metadata_members_keeps_exact_three_data_voters() {
    let fixture = EnforcedNetwork::new_with_metadata_members(4);
    let (key, range) = fixture.seed();
    let original = fixture.native.databases[&node("n0")]
        .read()
        .get("items", &key)
        .unwrap()
        .unwrap();
    let scope = fixture.actual_current_recovery_scope(&range);
    let parent = CancellationToken::uncancelable();
    for id in [node("n0"), node("n1"), node("n2")] {
        let prepared = fixture.clients[&node("n0")]
            .prepare_native_ready_observation(
                &id,
                &NativeReadyRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &parent,
            )
            .unwrap();
        assert_eq!(prepared.reply().range_index, 1);
        fixture.idle();
    }
    for id in [node("n0"), node("n1"), node("n2")] {
        let completed = fixture.clients[&node("n0")]
            .complete_native_recovery_observation(
                &id,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &parent,
            )
            .unwrap();
        assert_eq!(completed.reply().range_index, 1);
        fixture.idle();
    }
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    // A metadata-only member is not a fourth completion/data voter. A real
    // role producer must refuse it rather than manufacture local activation.
    assert!(fixture.runtimes[&node("n3")]
        .activate_native_recovery(
            &actor,
            &NativeCompletionRequest {
                scope: scope.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 3500,
            },
            &parent,
        )
        .is_err());
    fixture.idle();
    for id in [node("n1"), node("n2"), node("n0")] {
        let activated = fixture.runtimes[&id]
            .activate_native_recovery(
                &actor,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &parent,
            )
            .unwrap();
        assert_eq!(
            activated.reply().stage,
            NativeCompletionStage::ActivatedAnchor
        );
        assert!(activated.reply().activation_decision_sha256.is_some());
        fixture.idle();
        let db = fixture.native.databases[&id].read();
        let lineage = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(lineage.base_members().len(), 4);
        assert_eq!(lineage.physical_range_members().unwrap().len(), 3);
        let members = lineage.native_write_members().unwrap();
        assert_eq!(members.len(), 3);
        assert!(!members.contains_key(&node("n3")));
        assert_eq!(db.get("items", &key).unwrap().unwrap(), original);
    }
    fixture.coordinators[&node("n0")]
        .recover_local_leadership(8)
        .unwrap();
    let value = observed(fixture.read(fixture.request(&key, &range), &parent));
    assert_eq!(value.record().unwrap(), &original);
    assert_eq!(value.audit().range_index, 1);
    fixture.idle();
}
