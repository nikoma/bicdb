//! Actual producer/peer TLS controls. Activation is invoked locally with the
//! trusted current-member actor; there is no incoming Activate wire claim.
use super::*;
use crate::distribution_range_recovery::{NativeCompletionQuorumRound, NativeCompletionStage};
#[test]
fn actual_native_completion_quorum_rejects_duplicate_and_original_cancellation() {
    let (fixture, scope) = NetworkFixture::all_r1_prepared();
    for name in ["n0", "n1", "n2"] {
        fixture.complete(name, &scope).unwrap();
        fixture.wait_idle();
    }
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let mut duplicate =
        NativeCompletionQuorumRound::begin(fixture.ready_role(&scope), &parent).unwrap();
    duplicate
        .record(fixture.completion_status("n0", &scope).unwrap())
        .unwrap();
    fixture.wait_idle();
    assert!(duplicate
        .record(fixture.completion_status("n0", &scope).unwrap())
        .is_err());
    fixture.wait_idle();
    assert!(duplicate.finish().is_err());
    let mut cancelled =
        NativeCompletionQuorumRound::begin(fixture.ready_role(&scope), &parent).unwrap();
    for name in ["n0", "n1"] {
        cancelled
            .record(fixture.completion_status(name, &scope).unwrap())
            .unwrap();
        fixture.wait_idle();
    }
    assert!(cancelled.is_ready());
    parent.cancel();
    assert!(cancelled.finish().is_err());
    for name in ["n0", "n1", "n2"] {
        assert_eq!(
            fixture
                .completion_status(name, &scope)
                .unwrap()
                .reply()
                .stage,
            NativeCompletionStage::CompletedFrozen
        );
        fixture.wait_idle();
        assert!(fixture.native.databases[&node(name)]
            .read()
            .range_mutation_gate()
            .try_enter(scope.range_id)
            .is_err());
    }
}
#[test]
fn actual_native_same_voter_activation_releases_only_after_durable_decision_and_never_returns_ready(
) {
    let (fixture, scope) = NetworkFixture::all_r1_prepared();
    for name in ["n0", "n1", "n2"] {
        fixture.complete(name, &scope).unwrap();
        fixture.wait_idle();
    }
    let original = fixture
        .native
        .databases
        .iter()
        .map(|(n, db)| {
            (
                n.clone(),
                db.read()
                    .get("items", "actual-tls-historical")
                    .unwrap()
                    .unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut shas = BTreeMap::new();
    // Every peer's successful ActivatedAnchor remains usable as an immutable
    // COMPLETION decision for the subsequent recipient, never as NativeReady.
    for name in ["n1", "n2", "n0"] {
        let result = fixture.runtimes[&node(name)]
            .activate_native_recovery(
                &fixture.actor(),
                &fixture.completion_request(&scope),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        fixture.wait_idle();
        assert_eq!(result.reply().stage, NativeCompletionStage::ActivatedAnchor);
        assert_eq!(result.reply().range_index, 1);
        shas.insert(node(name), result.reply().native_serving_sha256.clone());
        assert!(fixture.status_ready(name, &scope).is_err());
        fixture.wait_idle();
        let db = fixture.native.databases[&node(name)].read();
        assert_eq!(
            db.get("items", "actual-tls-historical").unwrap().as_ref(),
            Some(&original[&node(name)])
        );
        let normal = db.range_mutation_gate().try_enter(scope.range_id).unwrap();
        drop(normal);
    }
    for name in ["n0", "n1", "n2"] {
        let repeated = fixture.runtimes[&node(name)]
            .activate_native_recovery(
                &fixture.actor(),
                &fixture.completion_request(&scope),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        fixture.wait_idle();
        assert_eq!(repeated.reply().native_serving_sha256, shas[&node(name)]);
        assert_eq!(repeated.reply().range_index, 1);
    }
    // Local exclusion release only. No recovered_epochs seeding, new ordinary
    // application ACK, StrongGet/current-head or physical HA proof is claimed.
}
