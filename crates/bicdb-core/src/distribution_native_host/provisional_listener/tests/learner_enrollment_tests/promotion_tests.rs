//! Genuine native stores/TLS. Diagnostic setup uses the existing enrollment
//! fixture; the separate CLI positive exercises the public single-command path.
use super::*;
use std::time::Instant;
fn config(f: &EnrollmentFixture) -> crate::DistributionConfig {
    let topology = f.launches[0].metadata.lock().committed_topology().clone();
    let local = &topology.nodes[&node(0)];
    crate::DistributionConfig {
        enabled: true,
        cluster_id: topology.cluster_id.clone(),
        range_image_bootstrap_id: topology.range_image_bootstrap_id,
        node_id: local.id.clone(),
        node_address: local.address.clone(),
        node_incarnation: local.incarnation,
        node_capacity_bytes: local.capacity_bytes,
        node_tls_certificate_sha256: local.tls_certificate_sha256.clone(),
        replication_factor: 3,
        initial_ranges: 4,
        ..Default::default()
    }
}

fn ready(
    f: &EnrollmentFixture,
    token: &CancellationToken,
) -> crate::distribution_consensus::NativeLearnerPromotionReadiness {
    let (mq, business) = f.current();
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_for_peer(&node(4), 16)
        .unwrap();
    let reply = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    assert!(reply.success && reply.native_provenance.is_some());
    let mut store = f.launches[0].metadata.lock();
    store.record_append_response(&node(4), &reply).unwrap();
    // A serde round-trip of the genuine response is intentionally NOT a sealed
    // observation. A claimed StoreUUID/frontier alone cannot promote a node.
    let decoded: crate::MetadataAppendResponse =
        serde_json::from_slice(&serde_json::to_vec(&reply).unwrap()).unwrap();
    assert!(store
        .observe_native_learner_promotion_readiness(&mq, business, &node(4), &decoded, token)
        .is_err());
    store
        .observe_native_learner_promotion_readiness(&mq, business, &node(4), &reply, token)
        .unwrap()
}
fn start_owned(f: &EnrollmentFixture, host: &mut NativeUnassignedLearnerHost) -> Uuid {
    let token = CancellationToken::uncancelable();
    f.launches[0]
        .enroll_registered_native_learner(&node(4), &token)
        .unwrap();
    let business = Uuid::now_v7();
    let own = f
        .learner_transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
        .unwrap();
    host.start_owned_metadata(&own, business, &token).unwrap()
}
#[test]
fn native_metadata_promotion_requires_actual_owned_ack_and_original_live_budget() {
    let f = EnrollmentFixture::new();
    let (_factory, mut host) = f.factory("actual-promotion");
    let token = CancellationToken::uncancelable();
    let configuration = config(&f);
    let id = start_owned(&f, &mut host);
    let original = token.child_with_deadline(Instant::now() + Duration::from_secs(5));
    let expired = ready(&f, &original);
    original.cancel();
    let (post, business) = f.current();
    let before = f.launches[0].metadata.lock().status();
    assert!(f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(&expired, &post, business, &configuration, 40, &token)
        .is_err());
    assert_eq!(
        f.launches[0].metadata.lock().status().last_log_index,
        before.last_log_index
    );
    let actual = ready(&f, &token);
    assert_eq!(actual.receiver_store_id(), id);
    let (post, business) = f.current();
    let entry = f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(&actual, &post, business, &configuration, 41, &token)
        .unwrap()
        .unwrap();
    // No proposal/list/queued response is an installed promotion.
    assert!(f.launches[0].metadata.lock().status().commit_index < entry.index);
    assert!(!host
        .test_owned_metadata()
        .unwrap()
        .lock()
        .status()
        .voters
        .contains(&node(4)));
    f.launches[0].heartbeat_initial_metadata(&token).unwrap();
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_from(entry.index, 16)
        .unwrap();
    let reply = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    f.launches[0]
        .metadata
        .lock()
        .record_append_response(&node(4), &reply)
        .unwrap();
    assert!(reply.success && reply.match_index >= entry.index);
    assert_eq!(
        reply.native_receiver.as_ref().unwrap().metadata_store_id(),
        id
    );
    let owned = host.test_owned_metadata().unwrap();
    assert!(owned.lock().status().commit_index >= entry.index);
    assert!(owned.lock().status().voters.contains(&node(4)));
    let business = Uuid::now_v7();
    let own = f
        .learner_transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
        .unwrap();
    host.validate_owned_metadata_member(&own, business, &token)
        .unwrap();
    // Promotion did not mutate the old immutable listener's allowlist.
    assert!(f
        .learner_transport
        .fetch_metadata_snapshot(&node(4))
        .is_err());
    let member = &own.committed_topology(business).unwrap().nodes[&node(4)];
    let mut member_config = configuration.clone();
    member_config.node_id = member.id.clone();
    member_config.node_address = member.address.clone();
    member_config.node_incarnation = member.incarnation;
    member_config.node_capacity_bytes = member.capacity_bytes;
    member_config.node_tls_certificate_sha256 = member.tls_certificate_sha256.clone();
    member_config.transport = f.networks[4].clone();
    host.enable_metadata_control(&own, business, member_config, &token)
        .unwrap();
    assert!(host.metadata_control_owned());
    let snapshot = f
        .learner_transport
        .fetch_metadata_snapshot(&node(4))
        .unwrap();
    assert!(snapshot.topology.nodes[&node(4)]
        .native_store_birth_v1
        .is_some());
    assert_eq!(
        snapshot.topology.nodes[&node(4)].metadata_role,
        MetadataMemberRole::Voter
    );
    // The SAME actual backend campaigns with real TLS votes/barrier, not a
    // forced role/term. Its current store UUID and original namespaces survive.
    host.elect_metadata_control(&token).unwrap();
    assert_eq!(
        owned.lock().status().role,
        crate::MetadataConsensusRole::Leader
    );
    assert_eq!(owned.lock().status().node_id, node(4));

    assert_eq!(
        owned
            .lock()
            .registered_native_metadata_member_store_id()
            .unwrap(),
        id
    );
    drop(owned);
    host.stop_owned_metadata(&token).unwrap();
    host.shutdown().unwrap();
    f.shutdown();
}
#[test]
fn native_metadata_promotion_pending_entry_and_held_ack_cannot_cross_actual_leader_transfer() {
    let f = EnrollmentFixture::new();
    let (_factory, mut host) = f.factory("actual-promotion-transfer");
    let token = CancellationToken::uncancelable();
    let configuration = config(&f);
    let id = start_owned(&f, &mut host);
    let held = ready(&f, &token);
    let (post, business) = f.current();
    let entry = f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(&held, &post, business, &configuration, 42, &token)
        .unwrap()
        .unwrap();
    let before = f.launches[0].metadata.lock().status();
    assert_eq!(before.last_log_index, entry.index);
    assert!(before.commit_index < entry.index);
    // Drop replication of this genuine pending promotion. The other three
    // actual old voters elect node1 using their real TLS votes and append a
    // current-term barrier; no term/log/ACK/counter is installed by the test.
    f.launches[1].elect_initial_metadata(&token).unwrap();
    f.launches[1].heartbeat_initial_metadata(&token).unwrap();
    let business = Uuid::now_v7();
    let current = f.launches[1]
        .transport
        .acquire_host_metadata_observation(&node(1), business, Duration::from_secs(5), &token)
        .unwrap();
    assert!(current.term() > before.current_term);
    let mut old = f.launches[0].metadata.lock();
    let topology = old.committed_topology().clone();
    let last = old.status().last_log_index;
    assert!(old
        .propose_native_learner_promotion(&held, &current, business, &configuration, 43, &token)
        .is_err());
    assert_eq!(old.committed_topology(), &topology);
    assert_eq!(old.status().last_log_index, last);
    assert_eq!(
        old.committed_topology().nodes[&node(4)].metadata_role,
        MetadataMemberRole::Learner
    );
    drop(old);
    assert_eq!(
        host.test_owned_metadata()
            .unwrap()
            .lock()
            .registered_native_metadata_member_store_id()
            .unwrap(),
        id
    );
    host.stop_owned_metadata(&token).unwrap();
    host.shutdown().unwrap();
    f.shutdown();
}
