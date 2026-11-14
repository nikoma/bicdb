//! Metadata-only data-binding tests: no serialized record is an admission proof.
use super::*;
use crate::distribution::{ClusterNode, DistributionConfig, DistributionStore, NativeStoreBirthV1};

fn topology() -> ClusterTopology {
    let root = tempfile::tempdir().unwrap();
    let config = DistributionConfig {
        enabled: true,
        cluster_id: ClusterId::new("birth-tests").unwrap(),
        node_id: ClusterNodeId::new("n1").unwrap(),
        node_address: "127.0.0.1:10001".into(),
        replication_factor: 1,
        initial_ranges: 1,
        ..DistributionConfig::default()
    };
    DistributionStore::initialize_at(root.path(), config, false, 1)
        .unwrap()
        .topology()
        .clone()
}
fn binding(bootstrap: Uuid, incarnation: u64) -> NativeStoreBirthV1 {
    serde_json::from_value(serde_json::json!({"version":1,"bootstrap_id":bootstrap,"birth_id":Uuid::new_v4(),"birth_sha256":"a".repeat(64),"node_incarnation":incarnation})).unwrap()
}
fn born() -> ClusterTopology {
    let mut t = topology();
    let b = Uuid::new_v4();
    t.range_image_bootstrap_id = Some(b);
    t.format_version = 2;
    t.nodes.values_mut().next().unwrap().native_store_birth_v1 = Some(binding(b, 1));
    t.validate().unwrap();
    t
}
fn open(root: &Path, t: &ClusterTopology) -> MetadataConsensusStore {
    MetadataConsensusStore::open(
        root,
        t.cluster_id.clone(),
        ClusterNodeId::new("n1").unwrap(),
        t.clone(),
        true,
    )
    .unwrap()
}
fn learner(t: &mut ClusterTopology) -> ClusterNodeId {
    let id = ClusterNodeId::new("n2").unwrap();
    t.nodes.insert(
        id.clone(),
        ClusterNode::new(id.clone(), "127.0.0.1:10002", 1, 1000, 1)
            .unwrap()
            .as_metadata_learner(),
    );
    id
}
fn entry(index: u64, t: ClusterTopology) -> MetadataLogEntry {
    MetadataLogEntry {
        index,
        term: 1,
        leadership_barrier: false,
        topology: t,
        restore_activation: None,
    }
}
fn append(t: &ClusterTopology, entries: Vec<MetadataLogEntry>) -> MetadataAppendRequest {
    MetadataAppendRequest {
        cluster_id: t.cluster_id.clone(),
        term: 1,
        leader_id: ClusterNodeId::new("n1").unwrap(),
        prev_log_index: 0,
        prev_log_term: 0,
        leader_commit: 0,
        snapshot: None,
        entries,
    }
}

#[test]
fn native_birth_shape_keeps_legacy_and_rejects_invalid_data_or_format() {
    let legacy = topology();
    assert_eq!(legacy.format_version, 1);
    assert!(!serde_json::to_string(&legacy)
        .unwrap()
        .contains("native_store_birth_v1"));
    let t = born();
    let b = t
        .nodes
        .values()
        .next()
        .unwrap()
        .native_store_birth_v1
        .as_ref()
        .unwrap();
    assert_eq!(b.version(), 1);
    assert_eq!(b.node_incarnation(), 1);
    let value = serde_json::to_value(b).unwrap();
    for (key, bad) in [
        ("version", serde_json::json!(2)),
        ("bootstrap_id", serde_json::json!(Uuid::nil())),
        ("birth_id", serde_json::json!(Uuid::nil())),
        ("birth_sha256", serde_json::json!("bad")),
        ("node_incarnation", serde_json::json!(2)),
    ] {
        let mut v = value.clone();
        v[key] = bad;
        let b: NativeStoreBirthV1 = serde_json::from_value(v).unwrap();
        assert!(
            b.validate_for(t.range_image_bootstrap_id, 1).is_err(),
            "{key}"
        );
    }
    let mut v = value;
    v["authority"] = serde_json::json!(true);
    assert!(serde_json::from_value::<NativeStoreBirthV1>(v).is_err());
    // Joint handoff adds format 3 while retaining format-2 birth bindings.
    // Unknown formats remain invalid; format 1 cannot carry those bindings.
    let mut joint_format = t.clone();
    joint_format.format_version = crate::distribution::NATIVE_JOINT_HANDOFF_TOPOLOGY_FORMAT_VERSION;
    joint_format.validate().unwrap();
    for format in [1, u32::MAX] {
        let mut wrong = t.clone();
        wrong.format_version = format;
        assert!(
            wrong.validate().is_err(),
            "unsupported birth format {format}"
        );
    }
    let mut wrong = t;
    wrong.range_image_bootstrap_id = Some(Uuid::new_v4());
    assert!(wrong.validate().is_err());
}

#[test]
fn native_birth_identity_tombstone_and_format_are_sticky() {
    let old = born();
    let id = ClusterNodeId::new("n1").unwrap();
    for change in 0..4 {
        let mut next = old.clone();
        next.generation += 1;
        match change {
            0 => next.nodes.get_mut(&id).unwrap().native_store_birth_v1 = None,
            1 => {
                next.nodes.get_mut(&id).unwrap().native_store_birth_v1 =
                    Some(binding(old.range_image_bootstrap_id.unwrap(), 1))
            }
            2 => {
                next.nodes.remove(&id);
            }
            _ => next.format_version = 1,
        }
        assert!(crate::distribution::validate_native_store_birth_transition(&old, &next).is_err());
        assert!(crate::distribution::validate_native_store_birth_snapshot(&old, &next).is_err());
    }
    let mut metrics = old.clone();
    metrics.nodes.get_mut(&id).unwrap().used_bytes = 1;
    assert!(crate::distribution::validate_native_store_birth_transition(&old, &metrics).is_ok());
    let mut empty = topology();
    empty.format_version = 2;
    let mut down = empty.clone();
    down.format_version = 1;
    assert!(crate::distribution::validate_native_store_birth_snapshot(&empty, &down).is_err());
}

#[test]
fn native_birth_live_generation_one_voter_cannot_auto_enroll_but_learner_can() {
    let mut old = topology();
    old.range_image_bootstrap_id = Some(Uuid::new_v4());
    let mut next = old.clone();
    next.generation += 1;
    next.format_version = 2;
    next.nodes
        .values_mut()
        .next()
        .unwrap()
        .native_store_birth_v1 = Some(binding(old.range_image_bootstrap_id.unwrap(), 1));
    assert!(crate::distribution::validate_native_store_birth_transition(&old, &next).is_err());
    assert!(crate::distribution::validate_native_store_birth_snapshot(&old, &next).is_err());
    let root = tempfile::tempdir().unwrap();
    let mut store = open(root.path(), &old);
    store.start_election().unwrap();
    store
        .become_leader(&BTreeSet::from([ClusterNodeId::new("n1").unwrap()]))
        .unwrap();
    assert!(store.propose_topology(next).is_err());
    let id = learner(&mut old);
    let mut next = old.clone();
    next.format_version = 2;
    next.generation += 1;
    next.nodes.get_mut(&id).unwrap().native_store_birth_v1 =
        Some(binding(old.range_image_bootstrap_id.unwrap(), 1));
    assert!(crate::distribution::validate_native_store_birth_transition(&old, &next).is_ok());
    next.nodes.get_mut(&id).unwrap().metadata_role = MetadataMemberRole::Voter;
    assert!(crate::distribution::validate_native_store_birth_transition(&old, &next).is_err());
    assert!(crate::distribution::validate_native_store_birth_snapshot(&old, &next).is_ok());
    let mut unknown = old.clone();
    unknown.nodes.remove(&id);
    assert!(crate::distribution::validate_native_store_birth_snapshot(&unknown, &next).is_ok());
}

#[test]
fn native_birth_replacement_requires_new_incarnation_and_live_learner_phase() {
    let mut old = born();
    let id = learner(&mut old);
    let node = old.nodes.get_mut(&id).unwrap();
    node.native_store_birth_v1 = Some(binding(old.range_image_bootstrap_id.unwrap(), 1));
    node.lifecycle = ClusterNodeLifecycle::Decommissioned;
    let mut next = old.clone();
    let node = next.nodes.get_mut(&id).unwrap();
    node.incarnation = 2;
    node.lifecycle = ClusterNodeLifecycle::Active;
    node.native_store_birth_v1 = Some(binding(old.range_image_bootstrap_id.unwrap(), 2));
    next.validate().unwrap();
    assert!(crate::distribution::validate_native_store_birth_transition(&old, &next).is_ok());
    next.nodes.get_mut(&id).unwrap().metadata_role = MetadataMemberRole::Voter;
    assert!(crate::distribution::validate_native_store_birth_transition(&old, &next).is_err());
    assert!(crate::distribution::validate_native_store_birth_snapshot(&old, &next).is_ok());
    next.nodes.get_mut(&id).unwrap().incarnation = 1;
    assert!(crate::distribution::validate_native_store_birth_snapshot(&old, &next).is_err());
}

#[test]
fn native_birth_append_validates_pending_sequence_before_any_mutation_or_overwrite() {
    let mut initial = born();
    let id = learner(&mut initial);
    let root = tempfile::tempdir().unwrap();
    let mut store = open(root.path(), &initial);
    let mut first = initial.clone();
    first.generation += 1;
    first.nodes.get_mut(&id).unwrap().native_store_birth_v1 =
        Some(binding(initial.range_image_bootstrap_id.unwrap(), 1));
    let mut rewrite = first.clone();
    rewrite.generation += 1;
    rewrite.nodes.get_mut(&id).unwrap().native_store_birth_v1 =
        Some(binding(initial.range_image_bootstrap_id.unwrap(), 1));
    let before = fs::read(root.path().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)).unwrap();
    assert!(store
        .handle_append_request(append(
            &initial,
            vec![entry(1, first.clone()), entry(2, rewrite.clone())]
        ))
        .is_err());
    assert!(store.state.log.is_empty());
    assert_eq!(
        fs::read(root.path().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)).unwrap(),
        before
    );
    store
        .handle_append_request(append(&initial, vec![entry(1, first.clone())]))
        .unwrap();
    let pending = store.state.clone();
    assert!(store
        .handle_append_request(append(&initial, vec![entry(1, rewrite)]))
        .unwrap_err()
        .to_string()
        .contains("same term and index"));
    assert_eq!(store.state, pending);
    drop(store);
    let reopened = open(root.path(), &initial);
    assert_eq!(reopened.state.log[0].topology, first);
    let mut forged = reopened.state.clone();
    let mut bad = forged.log[0].clone();
    bad.index = 2;
    bad.topology.generation += 1;
    bad.topology
        .nodes
        .get_mut(&id)
        .unwrap()
        .native_store_birth_v1 = None;
    forged.log.push(bad);
    assert!(validate_state(&forged).is_err());
}

#[test]
fn native_birth_snapshot_cache_and_reopen_preserve_known_bindings() {
    let initial = born();
    let root = tempfile::tempdir().unwrap();
    let mut store = open(root.path(), &initial);
    let mut wrong = initial.clone();
    wrong.generation += 1;
    wrong
        .nodes
        .values_mut()
        .next()
        .unwrap()
        .native_store_birth_v1 = None;
    // Snapshot installation is an INNER part of the actual append operation.
    // Test the public receiver boundary, which owns the original authority
    // operation and durable persistence, rather than mutating without it.
    let snapshot_request = |topology| MetadataAppendRequest {
        cluster_id: initial.cluster_id.clone(),
        term: 2,
        leader_id: ClusterNodeId::new("n1").unwrap(),
        prev_log_index: 10,
        prev_log_term: 2,
        entries: Vec::new(),
        leader_commit: 10,
        snapshot: Some(MetadataSnapshot {
            last_included_index: 10,
            last_included_term: 2,
            topology,
            restore_activation: None,
        }),
    };
    let state_before_invalid = store.state.clone();
    let version_before_invalid = store.test_actual_publication_version_evidence().unwrap();
    let invalid = store.handle_append_request(snapshot_request(wrong));
    let version_after_invalid = store.test_actual_publication_version_evidence().unwrap();
    eprintln!(
        "NATIVE_BIRTH_SNAPSHOT_INVALID_BEFORE_ASSERTIONS {}",
        serde_json::json!({
        "actual_error":invalid.as_ref().err().map(ToString::to_string),
        "version_before":version_before_invalid,"version_after":version_after_invalid,
        "state_unchanged":store.state==state_before_invalid,
        "metadata_DATA_unit_not_authenticated_network_or_HA":true})
    );
    assert!(invalid.is_err());
    assert_eq!(store.state, state_before_invalid);
    assert_eq!(version_before_invalid, version_after_invalid);
    assert_eq!(version_after_invalid["version"]["failed"], false);
    assert_eq!(version_after_invalid["version"]["ready"], true);
    assert_eq!(version_after_invalid["version"]["active_operations"], 0);
    let mut added = initial.clone();
    added.generation += 1;
    let id = learner(&mut added);
    let node = added.nodes.get_mut(&id).unwrap();
    node.metadata_role = MetadataMemberRole::Voter;
    node.native_store_birth_v1 = Some(binding(initial.range_image_bootstrap_id.unwrap(), 1));
    let accepted = store.handle_append_request(snapshot_request(added.clone()));
    let version_after_valid = store.test_actual_publication_version_evidence().unwrap();
    eprintln!(
        "NATIVE_BIRTH_SNAPSHOT_VALID_BEFORE_ASSERTIONS {}",
        serde_json::json!({
        "actual_error":accepted.as_ref().err().map(ToString::to_string),
        "actual_reply":accepted.as_ref().ok(),"version_after":version_after_valid,
        "committed_topology_matches":store.committed_topology()==&added,
        "metadata_DATA_unit_not_authenticated_network_or_HA":true})
    );
    assert!(accepted.unwrap().success);
    assert_eq!(store.committed_topology(), &added);
    assert_eq!(version_after_valid["version"]["failed"], false);
    assert_eq!(version_after_valid["version"]["ready"], true);
    assert_eq!(version_after_valid["version"]["active_operations"], 0);
    assert!(
        version_after_valid["version"]["generation"]
            .as_u64()
            .unwrap()
            > version_after_invalid["version"]["generation"]
                .as_u64()
                .unwrap()
    );
    store.persist().unwrap();
    drop(store);
    assert_eq!(open(root.path(), &initial).committed_topology(), &added);
    let mut wrong_bootstrap = initial.clone();
    wrong_bootstrap
        .nodes
        .values_mut()
        .next()
        .unwrap()
        .native_store_birth_v1 = Some(binding(initial.range_image_bootstrap_id.unwrap(), 1));
    assert!(MetadataConsensusStore::open(
        root.path(),
        initial.cluster_id.clone(),
        ClusterNodeId::new("n1").unwrap(),
        wrong_bootstrap,
        true
    )
    .is_err());
    // Cache initialized with the committed enrolled snapshot, not an unsafe legacy upgrade.
    let cache_root = tempfile::tempdir().unwrap();
    let config = DistributionConfig {
        enabled: true,
        cluster_id: initial.cluster_id.clone(),
        node_id: ClusterNodeId::new("n1").unwrap(),
        node_address: "127.0.0.1:10001".into(),
        range_image_bootstrap_id: initial.range_image_bootstrap_id,
        replication_factor: 1,
        initial_ranges: 1,
        ..DistributionConfig::default()
    };
    super::super::distribution::provision_cluster_member_directory(
        cache_root.path(),
        &crate::distribution::ClusterBootstrapSnapshot {
            topology: initial.clone(),
            config_template: config.clone(),
            metadata_leader_id: Some(ClusterNodeId::new("n1").unwrap()),
        },
        &ClusterNodeId::new("n1").unwrap(),
        crate::distribution::ClusterNetworkTransportConfig::default(),
        true,
    )
    .unwrap();
    let mut cache = DistributionStore::open(cache_root.path(), config, true).unwrap();
    cache.install_authoritative_topology(added.clone()).unwrap();
    let mut rewrite = added.clone();
    rewrite.generation += 1;
    rewrite
        .nodes
        .values_mut()
        .next()
        .unwrap()
        .native_store_birth_v1 = None;
    assert!(cache.install_authoritative_topology(rewrite).is_err());
    assert_eq!(cache.topology(), &added);
}

#[test]
fn native_birth_existing_metadata_cannot_upgrade_unbound_initial_voter_on_reopen() {
    let mut legacy = topology();
    legacy.range_image_bootstrap_id = Some(Uuid::new_v4());
    let root = tempfile::tempdir().unwrap();
    drop(open(root.path(), &legacy));
    let before = fs::read(root.path().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)).unwrap();
    let mut attempted = legacy.clone();
    attempted.format_version = 2;
    attempted
        .nodes
        .values_mut()
        .next()
        .unwrap()
        .native_store_birth_v1 = Some(binding(legacy.range_image_bootstrap_id.unwrap(), 1));
    assert!(MetadataConsensusStore::open(
        root.path(),
        legacy.cluster_id.clone(),
        ClusterNodeId::new("n1").unwrap(),
        attempted,
        true
    )
    .is_err());
    assert_eq!(
        fs::read(root.path().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)).unwrap(),
        before
    );
    assert!(open(root.path(), &legacy)
        .committed_topology()
        .nodes
        .values()
        .all(|node| node.native_store_birth_v1.is_none()));
}

/// Three fsynced metadata stores, actual vote/append/probe handlers, and no
/// canned quorum replies. This is an in-process Raft metadata regression;
/// it is not a native row, TLS, automatic-election or physical-fault test.
fn replace_uncommitted_birth_with_actual_majority(use_snapshot: bool) {
    let mut initial = born();
    for name in ["n2", "n3"] {
        let id = ClusterNodeId::new(name).unwrap();
        initial.nodes.insert(
            id.clone(),
            ClusterNode::new(id, format!("127.0.0.1:1000{}", &name[1..]), 1, 1000, 1).unwrap(),
        );
    }
    let learner_id = ClusterNodeId::new("n4").unwrap();
    initial.nodes.insert(
        learner_id.clone(),
        ClusterNode::new(learner_id.clone(), "127.0.0.1:10004", 1, 1000, 1)
            .unwrap()
            .as_metadata_learner(),
    );
    initial.validate().unwrap();
    let roots = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let mut stores = roots.iter().zip(["n1", "n2", "n3"]).map(|(root, name)| {
        MetadataConsensusStore::open(
            root.path(),
            initial.cluster_id.clone(),
            ClusterNodeId::new(name).unwrap(),
            initial.clone(),
            true,
        )
        .unwrap()
    });
    let mut m0 = stores.next().unwrap();
    let mut m1 = stores.next().unwrap();
    let mut m2 = stores.next().unwrap();
    let n0 = ClusterNodeId::new("n1").unwrap();
    let n1 = ClusterNodeId::new("n2").unwrap();
    let n2 = ClusterNodeId::new("n3").unwrap();
    let election = m0.start_election().unwrap();
    assert_eq!(election.term, 1);
    let mut votes = BTreeSet::from([n0.clone()]);
    for voter in [&mut m1, &mut m2] {
        let response = voter.handle_vote_request(election.clone()).unwrap();
        assert!(response.vote_granted);
        votes.insert(response.voter_id);
    }
    m0.become_leader(&votes).unwrap();
    for follower in [&mut m1, &mut m2] {
        let response = follower
            .handle_append_request(m0.append_request_from(1, 8).unwrap())
            .unwrap();
        let peer = response.node_id.clone();
        assert!(response.success);
        m0.record_append_response(&peer, &response).unwrap();
    }
    // Propagate the actual first-term committed barrier to both followers.
    for follower in [&mut m1, &mut m2] {
        assert!(
            follower
                .handle_append_request(m0.append_request_from(2, 8).unwrap())
                .unwrap()
                .success
        );
        assert_eq!(follower.status().commit_index, 1);
    }
    let mut proposed = initial.clone();
    proposed.generation += 1;
    proposed
        .nodes
        .get_mut(&learner_id)
        .unwrap()
        .native_store_birth_v1 = Some(binding(initial.range_image_bootstrap_id.unwrap(), 1));
    let uncommitted = m0.propose_topology(proposed.clone()).unwrap();
    assert_eq!(uncommitted.index, 2);
    assert_eq!(uncommitted.term, 1);
    assert_eq!(m0.status().commit_index, 1);
    assert!(m0.state.log.last().unwrap().topology.nodes[&learner_id]
        .native_store_birth_v1
        .is_some());
    assert!(m1.committed_topology().nodes[&learner_id]
        .native_store_birth_v1
        .is_none());
    assert!(m2.committed_topology().nodes[&learner_id]
        .native_store_birth_v1
        .is_none());
    // M0 is partitioned. M1 actually wins term 2 using M2's real vote.
    let election = m1.start_election().unwrap();
    assert_eq!(election.term, 2);
    let response = m2.handle_vote_request(election).unwrap();
    assert!(response.vote_granted);
    m1.become_leader(&BTreeSet::from([n1.clone(), response.voter_id]))
        .unwrap();
    let actual_overwrite = m1.append_request_from(2, 8).unwrap();
    assert!(actual_overwrite.snapshot.is_none());
    assert_eq!(actual_overwrite.entries.len(), 1);
    assert_eq!(actual_overwrite.entries[0].term, 2);
    let response = m2.handle_append_request(actual_overwrite.clone()).unwrap();
    assert!(response.success);
    assert!(m1.record_append_response(&n2, &response).unwrap());
    assert_eq!(m1.status().commit_index, 2);
    assert!(
        m2.handle_append_request(m1.append_request_from(3, 8).unwrap())
            .unwrap()
            .success
    );
    assert_eq!(m2.status().commit_index, 2);
    drop(m2); // The remaining live quorum must now be M0 + M1.
    let request = if use_snapshot {
        let request = m1.append_request_from(1, 8).unwrap();
        assert!(request.snapshot.is_some());
        request
    } else {
        actual_overwrite
    };
    let response = m0.handle_append_request(request).unwrap();
    assert!(response.success);
    m1.record_append_response(&n0, &response).unwrap();
    assert!(
        m0.handle_append_request(m1.append_request_from(3, 8).unwrap())
            .unwrap()
            .success
    );
    assert_eq!(m0.status().commit_index, 2);
    assert_eq!(m0.committed_topology(), m1.committed_topology());
    assert!(m0.committed_topology().nodes[&learner_id]
        .native_store_birth_v1
        .is_none());
    assert!(m0.state.log.is_empty());
    // Fresh request-correlated metadata quorum, not cached match indexes.
    let local = MetadataReadParticipant::from_transport(n1.clone(), 1, None, true);
    let mut context = m1
        .begin_read_authority(Uuid::new_v4(), Duration::from_secs(2), &local)
        .unwrap();
    let request = m1.read_authority_probe(&context, &n0, 3, 8).unwrap();
    let response = m0.handle_read_authority_probe(request.clone()).unwrap();
    let ack = MetadataReadAck::from_transport(
        &request,
        &n0,
        MetadataReadParticipant::from_transport(n0.clone(), 1, None, true),
        response,
    )
    .unwrap();
    assert!(m1.record_read_authority_ack(&mut context, &ack).unwrap());
    let authority = m1.finish_read_authority(context).unwrap();
    assert_eq!(authority.read_index(), 2);
    assert_eq!(authority.topology(), m0.committed_topology());
    drop(m0);
    let reopened = MetadataConsensusStore::open(
        roots[0].path(),
        initial.cluster_id.clone(),
        n0,
        initial,
        true,
    )
    .unwrap();
    assert_eq!(reopened.status().commit_index, 2);
    assert!(reopened.committed_topology().nodes[&learner_id]
        .native_store_birth_v1
        .is_none());
}

#[test]
fn native_birth_higher_term_majority_overwrites_only_uncommitted_binding_and_forms_fresh_quorum() {
    replace_uncommitted_birth_with_actual_majority(false);
}

#[test]
fn native_birth_committed_majority_snapshot_discards_only_uncommitted_binding_and_forms_fresh_quorum(
) {
    replace_uncommitted_birth_with_actual_majority(true);
}

fn anchor_from_actual_vote(
    leader: &mut MetadataConsensusStore,
    follower: &mut MetadataConsensusStore,
) {
    let vote = leader.start_election().unwrap();
    let response = follower.handle_vote_request(vote).unwrap();
    assert!(response.vote_granted);
    leader
        .become_leader(&BTreeSet::from([
            leader.state.node_id.clone(),
            response.voter_id,
        ]))
        .unwrap();
    let response = follower
        .handle_append_request(leader.append_request_from(1, 8).unwrap())
        .unwrap();
    assert!(response.success);
    let peer = response.node_id.clone();
    assert!(leader.record_append_response(&peer, &response).unwrap());
}

#[test]
fn native_birth_unbound_voter_two_step_demotion_enrollment_cannot_change_actual_store() {
    let mut initial = born();
    let old_voter = ClusterNodeId::new("n2").unwrap();
    initial.nodes.insert(
        old_voter.clone(),
        ClusterNode::new(old_voter.clone(), "127.0.0.1:10002", 1, 1000, 1).unwrap(),
    );
    initial.validate().unwrap();
    let root = tempfile::tempdir().unwrap();
    let peer_root = tempfile::tempdir().unwrap();
    let mut leader = open(root.path(), &initial);
    let mut peer = MetadataConsensusStore::open(
        peer_root.path(),
        initial.cluster_id.clone(),
        old_voter.clone(),
        initial.clone(),
        true,
    )
    .unwrap();
    anchor_from_actual_vote(&mut leader, &mut peer);
    let before = leader.state.clone();
    let bytes = fs::read(root.path().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)).unwrap();
    let mut demoted = initial.clone();
    demoted.generation += 1;
    demoted.nodes.get_mut(&old_voter).unwrap().metadata_role = MetadataMemberRole::Learner;
    demoted.validate().unwrap();
    assert!(leader
        .propose_topology(demoted.clone())
        .unwrap_err()
        .to_string()
        .contains("same incarnation"));
    assert_eq!(leader.state, before);
    assert_eq!(
        fs::read(root.path().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)).unwrap(),
        bytes
    );
    // The second step cannot smuggle a new birth into the rejected demotion.
    // This serialized DATA fixture grants no actual factory authority.
    let mut enrolled = demoted.clone();
    enrolled
        .nodes
        .get_mut(&old_voter)
        .unwrap()
        .native_store_birth_v1 = Some(binding(initial.range_image_bootstrap_id.unwrap(), 1));
    assert!(leader.propose_topology(enrolled).is_err());
    assert_eq!(leader.state, before);
    assert_eq!(
        fs::read(root.path().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)).unwrap(),
        bytes
    );
    assert!(leader
        .install_snapshot(MetadataSnapshot {
            last_included_index: 20,
            last_included_term: 2,
            topology: demoted,
            restore_activation: None
        })
        .is_err());
    assert_eq!(leader.state, before);
    let mut erased = initial.clone();
    erased.nodes.remove(&old_voter);
    assert!(crate::distribution::validate_native_store_birth_snapshot(&initial, &erased).is_err());
    assert_eq!(
        fs::read(root.path().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)).unwrap(),
        bytes
    );
    let id = ClusterNodeId::new("n4").unwrap();
    let mut fresh = initial.clone();
    fresh.generation += 1;
    let mut node = ClusterNode::new(id.clone(), "127.0.0.1:10004", 1, 1000, 1)
        .unwrap()
        .as_metadata_learner();
    node.native_store_birth_v1 = Some(binding(initial.range_image_bootstrap_id.unwrap(), 1));
    fresh.nodes.insert(id, node);
    fresh.validate().unwrap();
    assert!(
        leader.propose_topology(fresh).is_ok(),
        "genuinely unknown learner enrollment remains available"
    );
}

#[test]
fn native_birth_unbound_native_replacement_requires_new_incarnation_but_generic_legacy_demotion_remains(
) {
    let mut initial = born();
    let id = ClusterNodeId::new("n2").unwrap();
    initial.nodes.insert(
        id.clone(),
        ClusterNode::new(id.clone(), "127.0.0.1:10002", 1, 1000, 1).unwrap(),
    );
    let mut replacement = initial.clone();
    let node = replacement.nodes.get_mut(&id).unwrap();
    node.incarnation = 2;
    node.metadata_role = MetadataMemberRole::Learner;
    assert!(
        crate::distribution::validate_native_store_birth_transition(&initial, &replacement)
            .is_err(),
        "active predecessor cannot be replaced"
    );
    initial.nodes.get_mut(&id).unwrap().lifecycle = ClusterNodeLifecycle::Decommissioned;
    assert!(
        crate::distribution::validate_native_store_birth_transition(&initial, &replacement).is_ok(),
        "new-incarnation learner can replace a decommissioned unbound voter"
    );
    replacement.nodes.get_mut(&id).unwrap().incarnation = 1;
    assert!(
        crate::distribution::validate_native_store_birth_transition(&initial, &replacement)
            .is_err()
    );
    assert!(
        crate::distribution::validate_native_store_birth_snapshot(&initial, &replacement).is_err()
    );
    // Separate NEW legacy fixture, not a downgrade or migration of native state.
    let mut legacy = topology();
    legacy.nodes.insert(
        id.clone(),
        ClusterNode::new(id.clone(), "127.0.0.1:10002", 1, 1000, 1).unwrap(),
    );
    let root = tempfile::tempdir().unwrap();
    let peer_root = tempfile::tempdir().unwrap();
    let mut leader = open(root.path(), &legacy);
    let mut peer = MetadataConsensusStore::open(
        peer_root.path(),
        legacy.cluster_id.clone(),
        id.clone(),
        legacy.clone(),
        true,
    )
    .unwrap();
    anchor_from_actual_vote(&mut leader, &mut peer);
    let mut demoted = legacy.clone();
    demoted.generation += 1;
    demoted.nodes.get_mut(&id).unwrap().metadata_role = MetadataMemberRole::Learner;
    assert!(leader.propose_topology(demoted).is_ok());
    assert!(leader
        .state
        .log
        .iter()
        .all(|entry| entry.topology.range_image_bootstrap_id.is_none()
            && entry
                .topology
                .nodes
                .values()
                .all(|node| node.native_store_birth_v1.is_none())));
}

#[test]
fn native_abort_birth_capability_preserves_old_bytes_and_requires_writer_domain() {
    // Data-shape checks only. These DTOs do not stand in for actual factory evidence.
    let bootstrap = Uuid::new_v4();
    let old = binding(bootstrap, 1);
    let bytes = serde_json::to_vec(&old).unwrap();
    assert!(!String::from_utf8(bytes.clone())
        .unwrap()
        .contains("abort_protocol_version"));
    assert_eq!(old.abort_protocol_version(), 0);
    assert_eq!(
        serde_json::to_vec(&serde_json::from_slice::<NativeStoreBirthV1>(&bytes).unwrap()).unwrap(),
        bytes
    );
    let mut value = serde_json::to_value(&old).unwrap();
    value["abort_protocol_version"] = serde_json::json!(1);
    let no_writer: NativeStoreBirthV1 = serde_json::from_value(value.clone()).unwrap();
    assert!(no_writer.validate_for(Some(bootstrap), 1).is_err());
    value["writer_enforcement_version"] = serde_json::json!(1);
    let shape: NativeStoreBirthV1 = serde_json::from_value(value.clone()).unwrap();
    shape.validate_for(Some(bootstrap), 1).unwrap();
    assert_eq!(shape.abort_protocol_version(), 1);
    value["abort_protocol_version"] = serde_json::json!(2);
    assert!(serde_json::from_value::<NativeStoreBirthV1>(value)
        .unwrap()
        .validate_for(Some(bootstrap), 1)
        .is_err());
}
