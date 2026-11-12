//! Actual fsynced native/metadata stores with direct in-process Raft delivery.
//! No TLS, independent-host, data-rebalance or production qualification claim.
use super::*;
use crate::db::{ActualNativeRf3Fixture, RangeImageBirthMember, RangeImageBirthSpec};
use crate::distribution::ClusterNode;
use crate::{DbConfig, StorageMode};

fn node(i: usize) -> ClusterNodeId {
    ClusterNodeId::new(format!("n{i}")).unwrap()
}
fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}
fn replicate(stores: &mut BTreeMap<ClusterNodeId, MetadataConsensusStore>) {
    for _ in 0..3 {
        for i in [1, 2] {
            let request = stores[&node(0)].append_request_from(1, 64).unwrap();
            let ack = stores
                .get_mut(&node(i))
                .unwrap()
                .handle_append_request(request)
                .unwrap();
            assert!(ack.success);
            stores
                .get_mut(&node(0))
                .unwrap()
                .record_append_response(&node(i), &ack)
                .unwrap();
        }
    }
}

#[test]
fn native_metadata_learner_cannot_vote_and_cached_legacy_store_cannot_adopt_enforced_owner() {
    let f = ActualNativeRf3Fixture::new_with_enforced_network_and_ranges(&BTreeMap::new(), 1);
    let initial = f.topology.read().clone();
    let mut stores = (0..3)
        .map(|i| {
            let id = node(i);
            let store = MetadataConsensusStore::initialize_native_owned(
                f.root.path().join(format!("learner-meta-{id}")),
                Arc::clone(&f.databases[&id]),
                &f.fresh_factories[&id],
                initial.clone(),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
            (id, store)
        })
        .collect::<BTreeMap<_, _>>();
    let request = stores.get_mut(&node(0)).unwrap().start_election().unwrap();
    let mut votes = BTreeSet::from([node(0)]);
    for i in [1, 2] {
        let ack = stores
            .get_mut(&node(i))
            .unwrap()
            .handle_vote_request(request.clone())
            .unwrap();
        assert!(ack.vote_granted);
        votes.insert(ack.voter_id);
    }
    stores
        .get_mut(&node(0))
        .unwrap()
        .become_leader(&votes)
        .unwrap();
    replicate(&mut stores);

    let mut with_learner = stores[&node(0)].committed_topology().clone();
    let mut learner = ClusterNode::new(node(3), "127.0.0.1:19003", 1, 1_000_000, 1)
        .unwrap()
        .as_metadata_learner();
    learner.labels = with_learner.nodes[&node(0)].labels.clone();
    with_learner.nodes.insert(node(3), learner);
    with_learner.generation += 1;
    stores
        .get_mut(&node(0))
        .unwrap()
        .propose_topology(with_learner.clone())
        .unwrap();
    replicate(&mut stores);
    assert_eq!(stores[&node(0)].committed_topology(), &with_learner);

    let mut unbound_promotion = with_learner.clone();
    unbound_promotion.generation += 1;
    unbound_promotion
        .nodes
        .get_mut(&node(3))
        .unwrap()
        .metadata_role = MetadataMemberRole::Voter;
    let before_unbound = stores[&node(0)].status().last_log_index;
    let error = stores
        .get_mut(&node(0))
        .unwrap()
        .propose_topology(unbound_promotion)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires committed actual factory enrollment"),
        "{error}"
    );
    assert_eq!(stores[&node(0)].status().last_log_index, before_unbound);

    // Genuine pre-enrollment legacy learner: its own current configuration
    // permits metadata catch-up, never a vote for an eligible other candidate.
    let legacy_root = f.root.path().join("unenrolled-learner-meta");
    let mut legacy = MetadataConsensusStore::open(
        &legacy_root,
        initial.cluster_id.clone(),
        node(3),
        with_learner.clone(),
        true,
    )
    .unwrap();
    let caught_up = legacy
        .handle_append_request(stores[&node(0)].append_request_from(1, 64).unwrap())
        .unwrap();
    assert!(caught_up.success);
    let leader_status = stores[&node(0)].status();
    let vote = MetadataVoteRequest {
        cluster_id: initial.cluster_id.clone(),
        term: leader_status.current_term,
        candidate_id: node(0),
        last_log_index: leader_status.last_log_index,
        last_log_term: leader_status.last_log_term,
    };
    assert!(
        !legacy
            .handle_vote_request(vote.clone())
            .unwrap()
            .vote_granted
    );
    assert!(legacy.status().voted_for.is_none());
    let legacy_path = legacy_root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
    let before_binding = fs::read(&legacy_path).unwrap();

    // The declaration is produced by a REAL new unassigned native factory.
    // Possessing it in topology does not attach that physical store to an
    // unrelated already-open legacy metadata backend.
    let spec = RangeImageBirthSpec {
        cluster_id: initial.cluster_id.clone(),
        bootstrap_id: initial.range_image_bootstrap_id.unwrap(),
        node_id: node(3),
        member_incarnation: 1,
        restore_activation: None,
        initial_ranges: Vec::new(),
        initial_members: with_learner
            .nodes
            .iter()
            .map(|(id, m)| {
                (
                    id.clone(),
                    RangeImageBirthMember {
                        incarnation: m.incarnation,
                        tls_certificate_sha256: m.tls_certificate_sha256.clone(),
                        pending_tls_certificate_sha256: m.pending_tls_certificate_sha256.clone(),
                    },
                )
            })
            .collect(),
    };
    let data_path = f.root.path().join("actual-unassigned-native-n3");
    let (mut db, _) =
        BicDb::create_new_enrolled_range_image_database(&data_path, config(), spec.clone())
            .unwrap();
    db.create_collection("items").unwrap();
    drop(db);
    let (db, factory) = BicDb::open_range_image_database(&data_path, config(), &spec).unwrap();
    db.schema_compatibility_fingerprint().unwrap();
    let mut bound = with_learner.clone();
    crate::distribution_native_admission::ActualNativeStoreEnrollment::from_actual_factory(
        &factory, &db, &bound,
    )
    .unwrap()
    .bind_initial_or_unassigned_learner(&mut bound)
    .unwrap();
    bound.generation += 1;
    stores
        .get_mut(&node(0))
        .unwrap()
        .propose_topology(bound.clone())
        .unwrap();
    replicate(&mut stores);
    let mut premature = bound.clone();
    premature.generation += 1;
    premature.nodes.get_mut(&node(3)).unwrap().metadata_role = MetadataMemberRole::Voter;
    let before_premature = stores[&node(0)].status().last_log_index;
    let error = stores
        .get_mut(&node(0))
        .unwrap()
        .propose_topology(premature.clone())
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires actual caught-up metadata acknowledgement"),
        "{error}"
    );
    assert_eq!(stores[&node(0)].status().last_log_index, before_premature);

    let error = legacy
        .handle_append_request(stores[&node(0)].append_request_from(1, 64).unwrap())
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires actual native owned store"),
        "{error}"
    );
    assert_eq!(fs::read(&legacy_path).unwrap(), before_binding);
    assert!(legacy.handle_vote_request(vote.clone()).is_err());
    assert!(legacy.start_election().is_err());
    assert!(legacy.require_native_metadata_owner_current().is_err());
    let read_error = legacy
        .begin_read_authority(
            Uuid::now_v7(),
            Duration::from_secs(5),
            &MetadataReadParticipant::from_transport(node(3), 1, None, true),
        )
        .unwrap_err();
    assert!(read_error
        .to_string()
        .contains("requires actual native owned store"));
    assert!(legacy.native_owner.is_none());
    assert!(legacy.state.native_store_id.is_none());

    // Healthy positive: actual owned initialization binds the real native
    // root and its protected StoreUUID before acknowledging learner catch-up.
    let db = Arc::new(RwLock::new(db));
    let mut owned = MetadataConsensusStore::initialize_native_owned(
        f.root.path().join("actual-owned-native-learner-meta"),
        Arc::clone(&db),
        &factory,
        bound.clone(),
        &CancellationToken::uncancelable(),
    )
    .unwrap();
    let ack = owned
        .handle_append_request(stores[&node(0)].append_request_from(1, 64).unwrap())
        .unwrap();
    assert!(ack.success);
    assert_eq!(owned.committed_topology(), &bound);
    assert!(owned.state.native_store_id.is_some());
    assert!(
        !owned
            .handle_vote_request(MetadataVoteRequest {
                last_log_index: stores[&node(0)].status().last_log_index,
                last_log_term: stores[&node(0)].status().last_log_term,
                ..vote
            })
            .unwrap()
            .vote_granted
    );
    assert!(owned.start_election().is_err());
    stores
        .get_mut(&node(0))
        .unwrap()
        .record_append_response(&node(3), &ack)
        .unwrap();
    stores
        .get_mut(&node(0))
        .unwrap()
        .propose_topology(premature.clone())
        .unwrap();
    replicate(&mut stores);
    assert_eq!(stores[&node(0)].committed_topology(), &premature);
    let promoted_ack = owned
        .handle_append_request(stores[&node(0)].append_request_from(1, 64).unwrap())
        .unwrap();
    assert!(promoted_ack.success);
    assert_eq!(owned.committed_topology(), &premature);
    let current = stores[&node(0)].status();
    assert!(
        owned
            .handle_vote_request(MetadataVoteRequest {
                cluster_id: initial.cluster_id.clone(),
                term: current.current_term,
                candidate_id: node(0),
                last_log_index: current.last_log_index,
                last_log_term: current.last_log_term,
            })
            .unwrap()
            .vote_granted
    );
}
