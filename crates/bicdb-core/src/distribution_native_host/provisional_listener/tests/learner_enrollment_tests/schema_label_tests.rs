//! Genuine factory/TLS birth facts and fresh actual leader MQ; no label DTO
//! can become schema authority. This is metadata enrollment, never DATA Ready.
use super::*;

#[test]
fn native_registered_learner_schema_commits_actual_label_and_refuses_contradictory_or_bound_missing_label(
) {
    let f = EnrollmentFixture::new();
    let label = crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL;
    // Actual committed registration shape from join-fresh: no supplied schema
    // label. Use real proposal/old-voter heartbeat, not log/role setters.
    let mut registered = f.launches[0].metadata.lock().committed_topology().clone();
    registered
        .nodes
        .get_mut(&node(4))
        .unwrap()
        .labels
        .remove(label);
    f.commit(registered);
    let (factory, host) = f.factory("actual-unlabelled-learner");
    let parent = CancellationToken::uncancelable();
    let fact = f.observe(&parent);
    let schema = fact.schema_sha256().unwrap().to_owned();
    let (current, business) = f.current();
    let pending = {
        let mut store = f.launches[0].metadata.lock();
        let before = store.committed_topology().clone();
        assert!(before.nodes[&node(4)].native_store_birth_v1.is_none());
        assert!(!before.nodes[&node(4)].labels.contains_key(label));
        let pending = store
            .propose_observed_native_learner_enrollment(&fact, &current, business, &parent)
            .unwrap()
            .unwrap();
        assert_eq!(store.committed_topology(), &before);
        assert_eq!(
            pending.topology.nodes[&node(4)]
                .native_store_birth_v1
                .as_ref(),
            Some(fact.birth().unwrap())
        );
        assert_eq!(
            pending.topology.nodes[&node(4)].labels.get(label),
            Some(&schema)
        );
        pending
    };
    let (renewed, business) = f.current();
    let retried = f.launches[0]
        .metadata
        .lock()
        .propose_observed_native_learner_enrollment(&fact, &renewed, business, &parent)
        .unwrap()
        .unwrap();
    assert_eq!(
        retried, pending,
        "exact persisted pending schema/birth body, not a new index"
    );
    f.launches[0].heartbeat_initial_metadata(&parent).unwrap();
    for launch in &f.launches {
        let store = launch.metadata.lock();
        assert!(store.status().commit_index >= pending.index);
        let member = &store.committed_topology().nodes[&node(4)];
        assert_eq!(
            member.native_store_birth_v1.as_ref(),
            Some(fact.birth().unwrap())
        );
        assert_eq!(member.labels.get(label), Some(&schema));
        assert_eq!(member.metadata_role, MetadataMemberRole::Learner);
        assert!(store.committed_topology().ranges.values().all(|range| range
            .replicas
            .iter()
            .all(|replica| replica.node_id != node(4))));
    }
    assert!(!factory.database.read().commit_admission_ready());
    assert!(factory.spec.initial_ranges.is_empty());

    // Contradictory labels remain a refusal even though the actual birth and
    // schema are known. No overwrite via this necessary enrollment producer.
    let mut contradictory = f.launches[0].metadata.lock().committed_topology().clone();
    contradictory
        .nodes
        .get_mut(&node(4))
        .unwrap()
        .labels
        .insert(label.into(), "0".repeat(64));
    f.commit(contradictory);
    let fact = f.observe(&parent);
    f.reject_without_proposal(
        &fact,
        "learner schema label contradicts actual observed schema",
    );

    let mut missing = f.launches[0].metadata.lock().committed_topology().clone();
    missing
        .nodes
        .get_mut(&node(4))
        .unwrap()
        .labels
        .remove(label);
    f.commit(missing);
    let fact = f.observe(&parent);
    f.reject_without_proposal(
        &fact,
        "already bound learner lacks committed actual schema label",
    );
    host.shutdown().unwrap();
    f.shutdown();
}
