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
    let mut f = ActualNativeRf3Fixture::new_with_enforced_network_and_ranges(&BTreeMap::new(), 1);
    f.root.disable_cleanup(true);
    eprintln!(
        "NATIVE_DELAYED_PREBINDING_ACK_ROOT {}",
        f.root.path().display()
    );
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
    let before_pending_binding = fs::read(&legacy_path).unwrap();

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
    let bundle = f.databases[&node(0)]
        .read()
        .cluster_schema_bundle()
        .unwrap();
    let source_uid = f.databases[&node(0)]
        .read()
        .collections()
        .into_iter()
        .find(|meta| meta.name == "items")
        .unwrap()
        .local_namespace_id;
    let (mut db, fresh_factory) =
        BicDb::create_new_enrolled_range_image_database(&data_path, config(), spec.clone())
            .unwrap();
    db.initialize_unassigned_native_learner_schema(
        &fresh_factory,
        &with_learner,
        &bundle,
        &CancellationToken::uncancelable(),
    )
    .unwrap();
    assert!(db.scan_collection("items").unwrap().is_empty());
    let target_uid = db
        .collections()
        .into_iter()
        .find(|meta| meta.name == "items")
        .unwrap()
        .local_namespace_id;
    assert!(source_uid.is_some() && target_uid.is_some());
    assert_ne!(
        source_uid, target_uid,
        "new physical backing must not adopt source namespace identity"
    );
    assert!(
        db.install_cluster_schema_bundle(&bundle).is_err(),
        "ordinary native generic import stays refused"
    );
    assert!(
        db.initialize_unassigned_native_learner_schema(
            &fresh_factory,
            &with_learner,
            &bundle,
            &CancellationToken::uncancelable()
        )
        .is_err(),
        "existing schema is never overwritten as a fresh factory"
    );
    drop(fresh_factory);
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
    let enrollment = stores
        .get_mut(&node(0))
        .unwrap()
        .propose_topology(bound.clone())
        .unwrap();
    // Actual legacy learner ACKs the proposed entry BEFORE it is committed.
    // Hold that genuine returned response; do not record or edit its fields.
    let prebinding_request = stores[&node(0)].append_request_from(1, 64).unwrap();
    assert!(prebinding_request.leader_commit < enrollment.index);
    assert!(prebinding_request
        .entries
        .iter()
        .any(|entry| entry.index == enrollment.index));
    let held_prebinding_ack = legacy.handle_append_request(prebinding_request).unwrap();
    assert!(held_prebinding_ack.success);
    assert!(held_prebinding_ack.native_receiver.is_none());
    assert!(held_prebinding_ack.native_provenance.is_none());
    let held_json = serde_json::to_value(&held_prebinding_ack).unwrap();
    assert_eq!(
        held_json,
        serde_json::json!({
            "cluster_id": held_prebinding_ack.cluster_id,
            "term": held_prebinding_ack.term,
            "node_id": held_prebinding_ack.node_id,
            "success": true,
            "match_index": held_prebinding_ack.match_index,
            "conflict_index": held_prebinding_ack.conflict_index,
        }),
        "legacy response bytes/fields must not acquire native identity data"
    );
    assert_eq!(held_prebinding_ack.match_index, enrollment.index);
    assert_eq!(legacy.committed_topology(), &with_learner);
    assert!(legacy.native_owner.is_none() && legacy.state.native_store_id.is_none());
    let before_binding = fs::read(&legacy_path).unwrap();
    assert_ne!(
        before_binding, before_pending_binding,
        "actual pending enrollment entry was durably logged by the unbound learner"
    );
    assert_eq!(
        decode_state(&before_binding).unwrap().committed_topology,
        with_learner
    );
    eprintln!("NATIVE_DELAYED_PREBINDING_ACK held={held_prebinding_ack:?} enrollment={enrollment:?} unbound_status={:?}", legacy.status());
    replicate(&mut stores);
    assert_eq!(stores[&node(0)].committed_topology(), &bound);
    assert_eq!(stores[&node(0)].status().commit_index, enrollment.index);
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

    // The commit/reset has completed. A delayed ACK from the OLD unbound
    // request cannot become proof of current-birth OWNED metadata catch-up.
    // Exact baseline cdc remote execution exposed the missing source guard.
    // This unchanged safety assertion must pass only after owned provenance
    // is checked against the current committed native birth.
    let recorded = stores
        .get_mut(&node(0))
        .unwrap()
        .record_append_response(&node(3), &held_prebinding_ack);
    if let Err(error) = &recorded {
        let detail = error.to_string();
        assert!(detail.contains("native") && (detail.contains("birth") || detail.contains("owned")
            || detail.contains("bound") || detail.contains("identity")),
            "an unrelated I/O/persistence error is not the expected physical-identity refusal: {error}");
    }
    let after_delayed = stores
        .get_mut(&node(0))
        .unwrap()
        .propose_topology(premature.clone());
    eprintln!("NATIVE_DELAYED_PREBINDING_ACK after_commit recorded={recorded:?} promoted={after_delayed:?} leader={:?}", stores[&node(0)].status());
    assert!(after_delayed.is_err(),
        "delayed unbound ACK must NOT authorize native voter promotion; actual current-birth owned ACK is required");
    assert_eq!(stores[&node(0)].status().last_log_index, before_premature);
    assert_eq!(
        fs::read(&legacy_path).unwrap(),
        before_binding,
        "delivering a response cannot mutate the unrelated legacy learner file"
    );

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
    assert!(
        read_error
            .to_string()
            .contains("requires actual native owned store"),
        "{read_error}"
    );
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
    assert!(ack.native_receiver.is_some() && ack.native_provenance.is_some());
    assert_eq!(
        ack.native_receiver.as_ref().unwrap().metadata_store_id(),
        owned.state.native_store_id.unwrap()
    );
    assert_eq!(
        ack.native_receiver.as_ref().unwrap().native_birth(),
        bound.nodes[&node(3)]
            .native_store_birth_v1
            .as_ref()
            .unwrap()
    );
    assert_eq!(owned.committed_topology(), &bound);
    // Identity data round-trips, but the actual owned ACK producer does not.
    // Bare JSON and changes to any countable response field must refuse BEFORE
    // recording numeric progress; a genuine unchanged owned ACK still works.
    let serialized = serde_json::to_vec(&ack).unwrap();
    let bare: MetadataAppendResponse = serde_json::from_slice(&serialized).unwrap();
    assert_eq!(bare, ack);
    assert!(bare.native_receiver.is_some() && bare.native_provenance.is_none());
    let progress_before = stores[&node(0)].match_index.clone();
    let state_before = stores[&node(0)].state.clone();
    let bare_error = stores
        .get_mut(&node(0))
        .unwrap()
        .record_append_response(&node(3), &bare)
        .unwrap_err();
    assert!(bare_error.to_string().contains("not a DTO"), "{bare_error}");
    let mut modified = ack.clone();
    modified.match_index += 1;
    let modified_error = stores
        .get_mut(&node(0))
        .unwrap()
        .record_append_response(&node(3), &modified)
        .unwrap_err();
    assert!(
        modified_error.to_string().contains("body/pin mismatch"),
        "{modified_error}"
    );
    assert_eq!(stores[&node(0)].match_index, progress_before);
    assert_eq!(stores[&node(0)].state, state_before);
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
    // A fsynced ACK is a durable fact, not a lease on the receiver staying
    // alive. The actual owned store and physical DB can end after replying.
    drop(owned);
    drop(factory);
    drop(db);
    stores
        .get_mut(&node(0))
        .unwrap()
        .record_append_response(&node(3), &promoted_ack)
        .unwrap();
}
