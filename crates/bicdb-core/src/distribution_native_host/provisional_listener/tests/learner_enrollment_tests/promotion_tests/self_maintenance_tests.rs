//! Genuine owned stores, actual promotion ACK and real TLS election/barrier.
//! No supervisor runs here: the single finite maintenance call is the only
//! replication delivery after the before snapshot. Data remains unassigned.
use super::*;

#[test]
fn native_metadata_promoted_leader_maintenance_never_appends_to_itself() {
    let f = EnrollmentFixture::new();
    let (_factory, mut host) = f.factory("self-maintenance-baseline");
    let token = CancellationToken::uncancelable();
    let configuration = config(&f);
    let id = start_owned(&f, &mut host);
    let actual = ready(&f, &token);
    assert_eq!(actual.receiver_store_id(), id);
    let (post, business) = f.current();
    let entry = f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(&actual, &post, business, &configuration, 51, &token)
        .unwrap()
        .unwrap();
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
    assert!(reply.success && reply.match_index >= entry.index);
    assert_eq!(
        reply.native_receiver.as_ref().unwrap().metadata_store_id(),
        id
    );
    f.launches[0]
        .metadata
        .lock()
        .record_append_response(&node(4), &reply)
        .unwrap();
    let owned = host.test_owned_metadata().unwrap();
    assert!(owned.lock().status().voters.contains(&node(4)));
    assert!(owned.lock().status().commit_index >= entry.index);
    let business = Uuid::now_v7();
    let own = f
        .learner_transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
        .unwrap();
    host.validate_owned_metadata_member(&own, business, &token)
        .unwrap();
    let member = &own.committed_topology(business).unwrap().nodes[&node(4)];
    let mut member_config = configuration;
    member_config.node_id = member.id.clone();
    member_config.node_address = member.address.clone();
    member_config.node_incarnation = member.incarnation;
    member_config.node_capacity_bytes = member.capacity_bytes;
    member_config.node_tls_certificate_sha256 = member.tls_certificate_sha256.clone();
    member_config.transport = f.networks[4].clone();
    host.enable_metadata_control(&own, business, member_config, &token)
        .unwrap();
    host.elect_metadata_control(&token).unwrap();
    let before = owned.lock().status();
    assert_eq!(before.role, crate::MetadataConsensusRole::Leader);
    assert_eq!(before.leader_id, Some(node(4)));
    assert!(owned
        .lock()
        .committed_topology()
        .ranges
        .values()
        .all(|r| r.replicas.iter().all(|replica| replica.node_id != node(4))));
    let before_business = Uuid::now_v7();
    let fresh_before = f
        .learner_transport
        .acquire_host_metadata_observation(
            &node(4),
            before_business,
            Duration::from_secs(5),
            &token,
        )
        .unwrap();
    fresh_before.require_live_request(before_business).unwrap();
    assert_eq!(fresh_before.metadata_leader_id(), &node(4));
    // No fabricated target: exercise the ACTUAL automatic selector+consumer.
    // The only unassigned current member is the elected leader itself.
    let original = token.child_with_deadline(Instant::now() + Duration::from_secs(5));
    let report = host.maintain_metadata_control(&original);
    let after = owned.lock().status();
    let after_business = Uuid::now_v7();
    let fresh_after = f.learner_transport.acquire_host_metadata_observation(
        &node(4),
        after_business,
        Duration::from_secs(5),
        &original,
    );
    let summary = serde_json::json!({
        "source_revision":std::env::var("BICDB_TEST_SOURCE_SHA").ok(),
        "scope":"single finite actual owned metadata maintenance; no DATA or VM claim",
        "actual_store_id":id,"before":before,"after":after,
        "maintenance":report.as_ref().map(|r|serde_json::to_value(r).unwrap()).unwrap_or_else(|e|serde_json::json!({"error":e.to_string()})),
        "fresh_after":fresh_after.as_ref().map(|o|serde_json::json!({"term":o.term(),"read_index":o.read_index(),"context":o.context_id(),"leader":o.metadata_leader_id()})).unwrap_or_else(|e|serde_json::json!({"error":e.to_string()}))
    });
    println!(
        "NATIVE_METADATA_SELF_MAINTENANCE {}",
        serde_json::to_string(&summary).unwrap()
    );
    if let Some(root) = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT") {
        let path = std::path::PathBuf::from(root)
            .join(format!("native-self-maintenance-{}.json", Uuid::now_v7()));
        crate::storage::write_atomic(&path, &serde_json::to_vec_pretty(&summary).unwrap(), true)
            .unwrap();
    }
    // Close all real owners before reporting the intended invariant failure.
    // Fixture private files remain retained by its existing custody policy.
    drop(fresh_before);
    drop(owned);
    host.stop_owned_metadata(&token).unwrap();
    host.shutdown().unwrap();
    f.shutdown();
    assert_eq!(
        after.role,
        crate::MetadataConsensusRole::Leader,
        "automatic maintenance must not send a leader's Append back to the SAME owned store"
    );
    assert_eq!(after.current_term, before.current_term);
    assert_eq!(after.commit_index, before.commit_index);
    let report = report.unwrap();
    assert_eq!(report.node, None);
    assert_eq!(report.stage, "idle_or_backoff");
    fresh_after
        .unwrap()
        .require_live_request(after_business)
        .unwrap();
}
