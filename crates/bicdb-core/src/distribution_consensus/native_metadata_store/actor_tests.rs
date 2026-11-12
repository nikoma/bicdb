//! Actual born-v2 paged stores + actual fsynced Raft vote/Append/snapshot
//! delivery. Pins come from real generated certificates, but this is simulated
//! delivery, NOT authenticated TLS/physical HA or a data-handoff qualification.
use super::*;
use crate::db::{ActualNativeRf3Fixture, RangeImageBirthMember, RangeImageBirthSpec};
use crate::{DbConfig, StorageMode};
use rcgen::{CertificateParams, KeyPair};
fn node(i: usize) -> ClusterNodeId {
    ClusterNodeId::new(format!("n{i}")).unwrap()
}
fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}
fn pin() -> String {
    let key = KeyPair::generate().unwrap();
    let certificate = CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    hex::encode(Sha256::digest(certificate.der().as_ref()))
}
fn replicate(
    stores: &mut BTreeMap<ClusterNodeId, MetadataConsensusStore>,
    leader: &ClusterNodeId,
) -> Vec<String> {
    let mut refused = Vec::new();
    for _ in 0..3 {
        for i in [0, 2] {
            let id = node(i);
            let request = stores[leader].append_request_from(1, 32).unwrap();
            match stores.get_mut(&id).unwrap().handle_append_request(request) {
                Ok(reply) => {
                    stores
                        .get_mut(leader)
                        .unwrap()
                        .record_append_response(&id, &reply)
                        .unwrap();
                }
                Err(e) => refused.push(e.to_string()),
            }
        }
    }
    refused
}
#[test]
fn native_metadata_actor_reused_pin_cannot_rebind_old_root_but_retirement_persists() {
    let network = (0..3)
        .map(|i| (node(i), (format!("127.0.0.1:{}", 19100 + i), pin())))
        .collect::<BTreeMap<_, _>>();
    let mut f = ActualNativeRf3Fixture::new_with_enforced_network_and_ranges(&network, 1);
    f.root.disable_cleanup(true);
    eprintln!("NATIVE_METADATA_ACTOR_ROOT {}", f.root.path().display());
    let initial = f.topology.read().clone();
    let leader = node(1);
    let mut stores = (0..3)
        .map(|i| {
            let id = node(i);
            let store = MetadataConsensusStore::initialize_native_owned(
                f.root.path().join(format!("actor-meta-{id}")),
                Arc::clone(&f.databases[&id]),
                &f.fresh_factories[&id],
                initial.clone(),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
            (id, store)
        })
        .collect::<BTreeMap<_, _>>();
    let request = stores.get_mut(&leader).unwrap().start_election().unwrap();
    let mut votes = BTreeSet::from([leader.clone()]);
    for i in [0, 2] {
        let reply = stores
            .get_mut(&node(i))
            .unwrap()
            .handle_vote_request(request.clone())
            .unwrap();
        assert!(reply.vote_granted);
        votes.insert(reply.voter_id);
    }
    stores
        .get_mut(&leader)
        .unwrap()
        .become_leader(&votes)
        .unwrap();
    assert!(replicate(&mut stores, &leader).is_empty());
    let local = MetadataReadParticipant::from_transport(
        leader.clone(),
        1,
        Some(network[&leader].1.clone()),
        false,
    );
    let context = stores[&leader]
        .begin_read_authority(Uuid::now_v7(), Duration::from_secs(60), &local)
        .unwrap();
    let stale_probe = stores[&leader]
        .read_authority_probe(&context, &node(0), 1, 16)
        .unwrap();
    let mut retired = stores[&leader].committed_topology().clone();
    retired.generation += 1;
    retired.nodes.get_mut(&node(0)).unwrap().lifecycle = ClusterNodeLifecycle::Decommissioned;
    for range in retired.ranges.values_mut() {
        range.replicas.retain(|r| r.node_id != node(0));
        if range.leader == node(0) {
            range.leader = leader.clone();
        }
        range.epoch += 1;
    }
    retired.validate().unwrap();
    stores
        .get_mut(&leader)
        .unwrap()
        .propose_topology(retired.clone())
        .unwrap();
    let refusals = replicate(&mut stores, &leader);
    assert!(refusals
        .iter()
        .any(|e| e.contains("actual native metadata actor")));
    assert_eq!(stores[&node(0)].committed_topology(), &retired);
    assert_eq!(
        stores[&node(0)].status().role,
        MetadataConsensusRole::Follower
    );
    let saved = fs::read(
        f.root
            .path()
            .join("actor-meta-n0")
            .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
    )
    .unwrap();
    let same_candidate = MetadataVoteRequest {
        cluster_id: initial.cluster_id.clone(),
        term: 1,
        candidate_id: leader.clone(),
        last_log_index: stores[&leader].status().last_log_index,
        last_log_term: stores[&leader].status().last_log_term,
    };
    assert!(
        !stores
            .get_mut(&node(0))
            .unwrap()
            .handle_vote_request(same_candidate.clone())
            .unwrap()
            .vote_granted
    );
    assert!(stores.get_mut(&node(0)).unwrap().start_election().is_err());
    assert!(stores
        .get_mut(&node(0))
        .unwrap()
        .handle_read_authority_probe(stale_probe)
        .is_err());
    assert_eq!(stores[&node(0)].committed_topology(), &retired);
    assert_eq!(
        fs::read(
            f.root
                .path()
                .join("actor-meta-n0")
                .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
        )
        .unwrap(),
        saved
    );
    // Actual new higher-incarnation factory, deliberately REUSING the old
    // real-certificate pin. Neither node-id nor pin equality is native identity.
    let mut replacement = retired.clone();
    replacement.generation += 1;
    let old_pin = replacement.nodes[&node(0)].tls_certificate_sha256.clone();
    {
        let member = replacement.nodes.get_mut(&node(0)).unwrap();
        member.incarnation += 1;
        member.lifecycle = ClusterNodeLifecycle::Active;
        member.metadata_role = MetadataMemberRole::Learner;
        member.native_store_birth_v1 = None;
    }
    let members = replacement
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
        .collect();
    let spec = RangeImageBirthSpec {
        cluster_id: initial.cluster_id.clone(),
        bootstrap_id: initial.range_image_bootstrap_id.unwrap(),
        node_id: node(0),
        member_incarnation: 2,
        initial_members: members,
        restore_activation: None,
        initial_ranges: Vec::new(),
    };
    let path = f.root.path().join("replacement-birth-n0");
    let (mut db, _) =
        BicDb::create_new_enrolled_range_image_database(&path, config(), spec.clone()).unwrap();
    db.create_collection("items").unwrap();
    drop(db);
    let (db, factory) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
    crate::distribution_native_admission::ActualNativeStoreEnrollment::from_actual_factory(
        &factory,
        &db,
        &replacement,
    )
    .unwrap()
    .bind_initial_or_unassigned_learner(&mut replacement)
    .unwrap();
    assert_eq!(replacement.nodes[&node(0)].tls_certificate_sha256, old_pin);
    assert_ne!(
        replacement.nodes[&node(0)].native_store_birth_v1,
        initial.nodes[&node(0)].native_store_birth_v1
    );
    stores
        .get_mut(&leader)
        .unwrap()
        .propose_topology(replacement.clone())
        .unwrap();
    assert!(!replicate(&mut stores, &leader).is_empty());
    assert_eq!(
        stores[&node(0)].committed_topology(),
        &replacement,
        "restriction persists before any error acknowledgement"
    );
    // Healthy NEW actor can acknowledge learner catch-up, but cannot vote or
    // claim a fresh metadata authority until genuinely promoted.
    let db = Arc::new(RwLock::new(db));
    let mut new_store = MetadataConsensusStore::initialize_native_owned(
        f.root.path().join("replacement-meta-n0"),
        Arc::clone(&db),
        &factory,
        replacement.clone(),
        &CancellationToken::uncancelable(),
    )
    .unwrap();
    let ack = new_store
        .handle_append_request(stores[&leader].append_request_from(1, 32).unwrap())
        .unwrap();
    assert!(ack.success);
    assert!(new_store.start_election().is_err());
    assert_eq!(new_store.committed_topology(), &replacement);
    stores
        .get_mut(&leader)
        .unwrap()
        .record_append_response(&node(0), &ack)
        .unwrap();
    // Genuine committed promotion after the NEW physical learner caught up.
    // The OLD root must still refuse even when its node-id is again a voter.
    let mut promoted = replacement.clone();
    promoted.generation += 1;
    promoted.nodes.get_mut(&node(0)).unwrap().metadata_role = MetadataMemberRole::Voter;
    stores
        .get_mut(&leader)
        .unwrap()
        .propose_topology(promoted.clone())
        .unwrap();
    assert!(!replicate(&mut stores, &leader).is_empty());
    assert_eq!(stores[&node(0)].committed_topology(), &promoted);
    assert!(promoted.is_metadata_voter(&node(0)));
    assert_eq!(stores[&node(0)].status().voted_for, Some(leader.clone()));
    let current_last = stores[&leader].status().last_log_index;
    assert!(
        !stores
            .get_mut(&node(0))
            .unwrap()
            .handle_vote_request(MetadataVoteRequest {
                last_log_index: current_last,
                ..same_candidate
            })
            .unwrap()
            .vote_granted
    );
    assert!(stores.get_mut(&node(0)).unwrap().start_election().is_err());
    let saved_replacement = fs::read(
        f.root
            .path()
            .join("actor-meta-n0")
            .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
    )
    .unwrap();
    drop(stores.remove(&node(0)));
    assert!(MetadataConsensusStore::reopen_native_owned(
        f.root.path().join("actor-meta-n0"),
        Arc::clone(&f.databases[&node(0)]),
        node(0),
        initial.clone(),
        &CancellationToken::uncancelable()
    )
    .is_err());
    assert_eq!(
        fs::read(
            f.root
                .path()
                .join("actor-meta-n0")
                .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
        )
        .unwrap(),
        saved_replacement
    );
    let promoted_ack = new_store
        .handle_append_request(stores[&leader].append_request_from(1, 32).unwrap())
        .unwrap();
    assert!(promoted_ack.success);
    assert_eq!(new_store.committed_topology(), &promoted);
    assert_eq!(new_store.start_election().unwrap().candidate_id, node(0));
}
