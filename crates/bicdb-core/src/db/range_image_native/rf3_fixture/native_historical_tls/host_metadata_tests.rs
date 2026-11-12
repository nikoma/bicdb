//! Real mTLS and fresh metadata read-index contexts, without a range-read or
//! writer permission. Declared initial topology/elections are fixture setup.
use super::*;
use crate::distribution_consensus::{
    HostMetadataReadAcquireRequest, HostMetadataReadObservation,
    METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
};

fn request(node_id: &str, budget_ms: u64) -> HostMetadataReadAcquireRequest {
    HostMetadataReadAcquireRequest {
        protocol_version: METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
        acquisition_id: Uuid::now_v7(),
        read_request_id: Uuid::now_v7(),
        requester_node_id: node(node_id),
        requester_incarnation: 1,
        budget_ms,
    }
}
fn unassigned_learner(topology: &mut ClusterTopology, _: &ActualNativeRf3Fixture, _: &Path) {
    topology.nodes.get_mut(&node("n0")).unwrap().metadata_role =
        crate::distribution::MetadataMemberRole::Learner;
    for range in topology.ranges.values_mut() {
        range.replicas.retain(|r| r.node_id != node("n0"));
    }
    topology.validate().unwrap();
}
fn learner_fixture() -> NetworkFixture {
    NetworkFixture::new_with_initial_metadata_edit(
        true,
        true,
        false,
        "n1",
        "n2",
        false,
        Some(unassigned_learner),
    )
}

#[test]
fn host_metadata_actual_tls_follower_observes_without_generic_range_leadership() {
    let fixture = NetworkFixture::new_ready_network(true, true, false);
    let parent = CancellationToken::uncancelable();
    let business = Uuid::now_v7();
    let observation = fixture.clients[&node("n0")]
        .acquire_host_metadata_observation(&node("n2"), business, Duration::from_secs(5), &parent)
        .unwrap();
    observation.require_live_request(business).unwrap();
    assert_eq!(observation.requester_node_id(), &node("n0"));
    assert_eq!(observation.requester_incarnation(), 1);
    assert!(observation.term() > 0 && observation.read_index() > 0);
    assert!(!observation.context_id().is_nil());
    let topology = observation.committed_topology(business).unwrap();
    let range = topology.ranges.values().next().unwrap();
    assert_eq!(range.leader, node("n1"));
    assert!(fixture.clients[&node("n0")]
        .acquire_metadata_read_observation(
            &node("n2"),
            Uuid::now_v7(),
            range.id,
            range.epoch,
            Duration::from_secs(1),
            &parent
        )
        .is_err());
    assert!(observation.committed_topology(Uuid::now_v7()).is_err());
    fixture.wait_idle();
}

#[test]
fn host_metadata_actual_tls_metadata_leader_observes_over_its_actual_listener() {
    let fixture = NetworkFixture::new_ready_network(true, true, false);
    let business = Uuid::now_v7();
    let observation = fixture.clients[&node("n2")]
        .acquire_host_metadata_observation(
            &node("n2"),
            business,
            Duration::from_secs(5),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let topology = observation.committed_topology(business).unwrap();
    assert_eq!(observation.requester_node_id(), &node("n2"));
    assert_eq!(topology.ranges.values().next().unwrap().leader, node("n1"));
    assert!(observation.read_index() > 0 && observation.term() > 0);
    fixture.wait_idle();
}

#[test]
fn host_metadata_actual_tls_unassigned_learner_observes_but_cannot_supply_quorum() {
    let mut fixture = learner_fixture();
    let parent = CancellationToken::uncancelable();
    let business = Uuid::now_v7();
    let observation = fixture.clients[&node("n0")]
        .acquire_host_metadata_observation(&node("n2"), business, Duration::from_secs(5), &parent)
        .unwrap();
    let topology = observation.committed_topology(business).unwrap();
    assert_eq!(
        topology.nodes[&node("n0")].metadata_role,
        crate::distribution::MetadataMemberRole::Learner
    );
    assert!(topology
        .ranges
        .values()
        .all(|r| r.replicas.iter().all(|p| p.node_id != node("n0"))));
    fixture.wait_idle();
    // Real metadata majority was n1+n2. The learner cannot replace n1.
    let address = fixture.current.read().nodes[&node("n1")].address.clone();
    let index = fixture
        .servers
        .iter()
        .position(|s| s.local_addr().to_string() == address)
        .unwrap();
    fixture.servers.remove(index).join().unwrap();
    let start = Instant::now();
    assert!(fixture.clients[&node("n0")]
        .acquire_host_metadata_observation(
            &node("n2"),
            Uuid::now_v7(),
            Duration::from_millis(400),
            &parent
        )
        .is_err());
    assert!(start.elapsed() < Duration::from_secs(2));
    fixture.wait_idle();
}

#[test]
fn host_metadata_actual_tls_spoofed_member_incarnation_and_missing_member_refuse() {
    let fixture = NetworkFixture::new_ready_network(true, true, false);
    let parent = CancellationToken::uncancelable();
    for mut forged in [
        request("n1", 1000),
        request("n0", 1000),
        request("unknown", 1000),
    ] {
        if forged.requester_node_id == node("n0") {
            forged.requester_incarnation += 1;
        }
        assert!(fixture.clients[&node("n0")]
            .request_host_metadata_reply(&node("n2"), &forged, &parent)
            .is_err());
    }
    // An actual follower remains able to acquire afterward.
    let business = Uuid::now_v7();
    fixture.clients[&node("n0")]
        .acquire_host_metadata_observation(&node("n2"), business, Duration::from_secs(5), &parent)
        .unwrap()
        .require_live_request(business)
        .unwrap();
    fixture.wait_idle();
}

#[test]
fn host_metadata_actual_tls_response_cannot_be_retagged_under_new_request() {
    let fixture = NetworkFixture::new_ready_network(true, true, false);
    let parent = CancellationToken::uncancelable();
    let original = request("n0", 5000);
    let authenticated = fixture.clients[&node("n0")]
        .request_host_metadata_reply(&node("n2"), &original, &parent)
        .unwrap();
    let mut replacement = original.clone();
    replacement.acquisition_id = Uuid::now_v7();
    assert!(HostMetadataReadObservation::from_transport(authenticated, &replacement).is_err());
    let authenticated = fixture.clients[&node("n0")]
        .request_host_metadata_reply(&node("n2"), &original, &parent)
        .unwrap();
    replacement = original.clone();
    replacement.read_request_id = Uuid::now_v7();
    assert!(HostMetadataReadObservation::from_transport(authenticated, &replacement).is_err());
    fixture.wait_idle();
}

#[test]
fn host_metadata_actual_tls_cancellation_and_delayed_decode_preserve_original_budget() {
    let fixture = NetworkFixture::new_ready_network(true, true, false);
    let parent = CancellationToken::new(Arc::new(AtomicBool::new(false)), None);
    let business = Uuid::now_v7();
    let observation = fixture.clients[&node("n0")]
        .acquire_host_metadata_observation(&node("n2"), business, Duration::from_secs(5), &parent)
        .unwrap();
    observation.require_live_request(business).unwrap();
    parent.cancel();
    assert!(observation.require_live_request(business).is_err());
    let parent = CancellationToken::new(
        Arc::new(AtomicBool::new(false)),
        Some(Instant::now() + Duration::from_millis(500)),
    );
    let original = request("n0", 500);
    let authenticated = fixture.clients[&node("n0")]
        .request_host_metadata_reply(&node("n2"), &original, &parent)
        .unwrap();
    while parent.check().is_ok() {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(HostMetadataReadObservation::from_transport(authenticated, &original).is_err());
    fixture.wait_idle();
}
