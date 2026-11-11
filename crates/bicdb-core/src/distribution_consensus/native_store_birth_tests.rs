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
    for format in [1, 3] {
        let mut wrong = t.clone();
        wrong.format_version = format;
        assert!(wrong.validate().is_err());
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
        .is_err());
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
    assert!(store
        .install_snapshot(MetadataSnapshot {
            last_included_index: 10,
            last_included_term: 2,
            topology: wrong,
            restore_activation: None
        })
        .is_err());
    let mut added = initial.clone();
    added.generation += 1;
    let id = learner(&mut added);
    let node = added.nodes.get_mut(&id).unwrap();
    node.metadata_role = MetadataMemberRole::Voter;
    node.native_store_birth_v1 = Some(binding(initial.range_image_bootstrap_id.unwrap(), 1));
    store
        .install_snapshot(MetadataSnapshot {
            last_included_index: 10,
            last_included_term: 2,
            topology: added.clone(),
            restore_activation: None,
        })
        .unwrap();
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
