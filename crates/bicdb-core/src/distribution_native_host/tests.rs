//! Actual local seed/factory checks. Placement tests validate initial config;
//! they are not online relocation or physical qualification.
use super::*;
use crate::{ClusterId, ClusterNode, DistributionConfig, Record};

fn node(name: &str) -> ClusterNodeId {
    ClusterNodeId::new(name).unwrap()
}
fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}
fn plan(count: u64, ranges: u32) -> NativeClusterBootstrapPlan {
    let members = (0..count)
        .map(|i| {
            ClusterNode::new(
                node(&format!("n{i}")),
                format!("127.0.0.1:{}", 16000 + i),
                1,
                1 << 30,
                10,
            )
            .unwrap()
            .with_tls_certificate_sha256(format!("{:064x}", i + 1))
            .unwrap()
            .with_label("server", format!("s{i}"))
            .unwrap()
        })
        .collect::<Vec<_>>();
    let config = DistributionConfig {
        enabled: true,
        cluster_id: ClusterId::new("native-host-bootstrap-test").unwrap(),
        range_image_bootstrap_id: Some(Uuid::now_v7()),
        node_id: node("n0"),
        node_address: members[0].address.clone(),
        node_tls_certificate_sha256: members[0].tls_certificate_sha256.clone(),
        node_incarnation: 1,
        node_capacity_bytes: 1 << 30,
        replication_factor: 3,
        initial_ranges: ranges,
        ..Default::default()
    };
    NativeClusterBootstrapPlan::new(
        &config,
        members,
        vec![NativeBootstrapCollection {
            name: "items".into(),
            mode: CollectionMode::Standard,
        }],
        10,
        &CancellationToken::uncancelable(),
    )
    .unwrap()
}

#[test]
fn native_host_initial_plan_assigns_replicas_and_leaders_without_manual_shards() {
    for count in [3, 4, 5] {
        let plan = plan(count, 64);
        let mut replicas = std::collections::BTreeMap::new();
        let mut leaders = std::collections::BTreeMap::new();
        for range in plan.topology.ranges.values() {
            assert_eq!(range.epoch, 1);
            assert_eq!(range.voter_count(), 3);
            assert!(range.replicas.iter().any(|r| r.node_id == range.leader));
            for replica in &range.replicas {
                *replicas.entry(replica.node_id.clone()).or_insert(0usize) += 1;
            }
            *leaders.entry(range.leader.clone()).or_insert(0usize) += 1;
        }
        assert_eq!(replicas.len(), count as usize);
        assert_eq!(leaders.len(), count as usize);
        assert!(replicas.values().max().unwrap() - replicas.values().min().unwrap() <= 1);
        assert!(leaders.values().max().unwrap() - leaders.values().min().unwrap() <= 1);
        assert!(plan
            .topology
            .nodes
            .values()
            .all(|n| n.native_store_birth_v1.is_none()));
        assert!(plan.topology.relocations.is_empty());
    }
}

#[test]
fn native_host_actual_seed_strict_reopen_preserves_birth_and_still_cannot_ack() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("native");
    let initial = plan(3, 4);
    let seed = NativeProvisionalNode::create(
        &root,
        config(),
        initial.clone(),
        &node("n0"),
        &CancellationToken::uncancelable(),
    )
    .unwrap();
    let birth = seed.manifest.birth_id;
    let uid = seed
        .database
        .read()
        .local_namespace_identity("items")
        .unwrap();
    {
        let db = seed.database.read();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", Record::new("must-not-ack-before-enrollment"))
            .unwrap();
        assert!(tx.commit().is_err());
        assert!(db
            .get("items", "must-not-ack-before-enrollment")
            .unwrap()
            .is_none());
    }
    drop(seed);
    let reopened = NativeProvisionalNode::open_unregistered(
        &root,
        config(),
        initial.clone(),
        &node("n0"),
        &CancellationToken::uncancelable(),
    )
    .unwrap();
    assert_eq!(reopened.manifest.birth_id, birth);
    assert_eq!(
        reopened
            .database
            .read()
            .local_namespace_identity("items")
            .unwrap(),
        uid
    );
    for range in &reopened.spec.initial_ranges {
        assert_eq!(
            reopened
                .database
                .read()
                .native_range_prefix_observation(range.id)
                .unwrap()
                .native_applied_index,
            Some(0)
        );
    }
    drop(reopened);
    // Any metadata state prohibits treating this as an unregistered seed.
    std::fs::write(
        root.join(crate::distribution::DEFAULT_CLUSTER_TOPOLOGY),
        b"refusal-only sentinel",
    )
    .unwrap();
    assert!(NativeProvisionalNode::open_unregistered(
        &root,
        config(),
        initial,
        &node("n0"),
        &CancellationToken::uncancelable()
    )
    .err()
    .unwrap()
    .to_string()
    .contains("metadata already exists"));
}

#[test]
fn native_host_initial_seed_refuses_existing_root_and_incompatible_plan() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("existing");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("sentinel"), b"keep").unwrap();
    let initial = plan(3, 4);
    assert!(NativeProvisionalNode::create(
        &root,
        config(),
        initial.clone(),
        &node("n0"),
        &CancellationToken::uncancelable()
    )
    .is_err());
    assert_eq!(std::fs::read(root.join("sentinel")).unwrap(), b"keep");
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    let mut invalid = initial.clone();
    invalid.topology.ranges.values_mut().next().unwrap().epoch = 2;
    let other = parent.path().join("never-created");
    assert!(NativeProvisionalNode::create(
        &other,
        config(),
        invalid,
        &node("n0"),
        &CancellationToken::uncancelable()
    )
    .is_err());
    assert!(!other.exists());
    let mut invalid = initial;
    invalid.collections[0].name = "invalid.name".into();
    assert!(NativeProvisionalNode::create(
        &other,
        config(),
        invalid,
        &node("n0"),
        &CancellationToken::uncancelable()
    )
    .is_err());
    assert!(!other.exists());
}
