//! Bootstrap is routing DATA, and a stale seed's
//! missing member must not preclude discovering a live current Host MQ.
//! No supervisor runs here; every election uses real stores/TLS votes and a
//! durable current-term barrier. The old seed misses only actual replication.
use super::*;

#[test]
fn native_metadata_fresh_sixth_stale_seed_discovers_current_quorum_before_root() {
    let mut f = EnrollmentFixture::with_spares(2);
    let (_factory, mut fifth) = f.factory("stale-seed-fifth");
    let token = CancellationToken::uncancelable();
    let mut configuration = config(&f);
    let fifth_uuid = start_owned(&f, &mut fifth);
    let actual = ready(&f, &token);
    let (post, business) = f.current();
    let promotion = f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(&actual, &post, business, &configuration, 91, &token)
        .unwrap()
        .unwrap();
    f.launches[0].heartbeat_initial_metadata(&token).unwrap();
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_from(promotion.index, 16)
        .unwrap();
    let reply = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    assert!(reply.success && reply.match_index >= promotion.index);
    f.launches[0]
        .metadata
        .lock()
        .record_append_response(&node(4), &reply)
        .unwrap();
    let business = Uuid::now_v7();
    let own = f
        .learner_transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
        .unwrap();
    fifth
        .validate_owned_metadata_member(&own, business, &token)
        .unwrap();
    let member = &own.committed_topology(business).unwrap().nodes[&node(4)];
    let mut fifth_config = configuration.clone();
    fifth_config.node_id = member.id.clone();
    fifth_config.node_address = member.address.clone();
    fifth_config.node_incarnation = member.incarnation;
    fifth_config.node_capacity_bytes = member.capacity_bytes;
    fifth_config.node_tls_certificate_sha256 = member.tls_certificate_sha256.clone();
    fifth_config.transport = f.networks[4].clone();
    fifth
        .enable_metadata_control(&own, business, fifth_config, &token)
        .unwrap();

    // Install the OLD seed's genuine control surface by the supported same-
    // service drain, retaining its actual store independently for assertions.
    configuration.transport = f.networks[0].clone();
    let old_metadata = Arc::clone(&f.launches[0].metadata);
    let old_launch = f.launches.remove(0);
    let old_listener = f.listeners.remove(0);
    let mut old_control = crate::distribution_native_host::NativeMetadataControlHost::new(
        old_launch,
        old_listener,
        f.networks[0].clone(),
        configuration.clone(),
    );
    old_control.advance(&token).unwrap();
    fifth.elect_metadata_control(&token).unwrap();
    let current_store = fifth.test_owned_metadata().unwrap();
    assert_eq!(current_store.lock().status().node_id, node(4));
    assert_eq!(
        current_store.lock().status().role,
        crate::MetadataConsensusRole::Leader
    );
    assert_eq!(
        current_store
            .lock()
            .registered_native_metadata_member_store_id()
            .unwrap(),
        fifth_uuid
    );
    let c5 = old_metadata.lock().committed_topology().clone();
    assert_eq!(c5.nodes.len(), 5);
    assert!(!c5.nodes.contains_key(&node(5)));
    assert_eq!(old_metadata.lock().status().leader_id, Some(node(4)));
    old_control.stop(&token).unwrap();

    // A sixth real TLS identity registers through actual leader4. With seed0
    // stopped, old voters1+2 and new leader4 genuinely commit the CONFIG.
    let client = crate::TcpClusterBootstrapClient::new(
        configuration.cluster_id.clone(),
        node(5),
        f.networks[5].clone(),
    )
    .unwrap();
    let address4 = c5.nodes[&node(4)].address.clone();
    let address0 = c5.nodes[&node(0)].address.clone();
    let address5 = f.next_learner_address.as_ref().unwrap().clone();
    let now: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    let sixth = ClusterNode::new(node(5), address5.clone(), 1, 1 << 30, now)
        .unwrap()
        .as_metadata_learner()
        .with_tls_certificate_sha256(
            crate::replication_transport::replication_certificate_sha256(
                &f.networks[5].tls.as_ref().unwrap().cert_path,
            )
            .unwrap(),
        )
        .unwrap();
    let c6 = client
        .register_metadata_learner(&node(4), &address4, sixth, now)
        .unwrap();
    assert_eq!(c6.nodes.len(), 6);
    let registration = c6
        .history
        .iter()
        .find(|event| event.summary == "node n5 joined as metadata Learner")
        .unwrap();
    assert_eq!(registration.actor, node(4));
    assert!(registration.generation > c5.generation);
    assert!(c6.generation > c5.generation);
    assert_eq!(c6.ranges, c5.ranges);
    assert_eq!(current_store.lock().committed_topology(), &c6);
    // Deliver real leader-built postcommit Append only to1/2; do not use a
    // catalogue/state/commit-index edit to make the witness quorum current.
    f.learner_transport.install_topology(c6.clone()).unwrap();
    for peer in [node(1), node(2)] {
        let request = current_store
            .lock()
            .append_request_for_peer(&peer, 16)
            .unwrap();
        let reply = f.learner_transport.append_metadata(&peer, request).unwrap();
        assert!(reply.success && reply.native_provenance.is_some());
        current_store
            .lock()
            .record_append_response(&peer, &reply)
            .unwrap();
        let actual_peer = f
            .launches
            .iter()
            .find(|launch| launch.metadata.lock().status().node_id == peer)
            .unwrap();
        assert_eq!(actual_peer.metadata.lock().committed_topology(), &c6);
    }
    assert_eq!(old_metadata.lock().committed_topology(), &c5);
    let current = client.fetch_snapshot(&node(4), &address4).unwrap();
    assert_eq!(current.topology, c6);

    // Independent positive: leader4 actually supplies fresh OWN MQ and schema
    // to caller6. This is no claim that stale data/schema source0 can serve6.
    let transport = TcpClusterRelocationTransport::new(
        configuration.cluster_id.clone(),
        node(5),
        Arc::new(RwLock::new(c6.clone())),
        f.networks[5].clone(),
    )
    .unwrap();
    let business = Uuid::now_v7();
    let fresh = transport
        .acquire_host_metadata_observation(&node(4), business, Duration::from_secs(5), &token)
        .unwrap();
    assert_eq!(fresh.committed_topology(business).unwrap(), &c6);
    assert_eq!(fresh.metadata_leader_id(), &node(4));
    let range = c6.ranges.values().next().unwrap();
    transport
        .export_native_learner_schema(&node(4), range, &token)
        .unwrap()
        .verify()
        .unwrap();
    assert_eq!(old_metadata.lock().committed_topology(), &c5);
    // Fresh MQ, not cached bootstrap identity, must reject wrong own endpoint
    // or incarnation BEFORE schema/factory effects. Keep0 stopped throughout.
    for (name, address, incarnation) in [
        ("wrong-endpoint", address0.as_str(), 1),
        ("wrong-incarnation", address5.as_str(), 2),
    ] {
        let refused_root = f.root.path().join(name);
        let error = crate::NativeMetadataLearnerHost::start_fresh_metadata_only(
            &refused_root,
            configuration.cluster_id.clone(),
            node(5),
            address,
            incarnation,
            node(4),
            &address4,
            f.networks[5].clone(),
            8 * 1024 * 1024,
            &token,
        )
        .err()
        .expect("fresh actual identity mismatch must refuse");
        assert!(
            error
                .to_string()
                .contains("registration/address/incarnation/birth differs"),
            "{error}"
        );
        assert!(!refused_root.exists());
    }
    // Keep0 offline through BOTH independent positive controls: a successful
    // current-MQ probe can itself replicate missing CONFIG entries. Restart
    // the SAME backend only now, so constructor discovery sees actual stale C5.
    old_control.advance(&token).unwrap();
    let stale = client.fetch_snapshot(&node(0), &address0).unwrap();
    assert_eq!(stale.topology, c5);
    assert_eq!(stale.metadata_leader_id, Some(node(4)));
    assert!(!stale.topology.nodes.contains_key(&node(5)));

    let root = f.root.path().join("sixth-via-stale-seed");
    assert!(!root.exists());
    let attempt = token.child_with_deadline(Instant::now() + Duration::from_secs(5));
    let started = Instant::now();
    let result = crate::NativeMetadataLearnerHost::start_fresh_metadata_only(
        &root,
        configuration.cluster_id.clone(),
        node(5),
        &address5,
        1,
        node(0),
        &address0,
        f.networks[5].clone(),
        8 * 1024 * 1024,
        &attempt,
    );
    let error = result.as_ref().err().map(ToString::to_string);
    let report = serde_json::json!({
        "version": 1,
        "stage": "actual_fresh_constructor_returned",
        "stale_seed": "n0",
        "stale_generation": c5.generation,
        "current_leader": "n4",
        "current_generation": c6.generation,
        "caller": "n5",
        "current_own_mq_term": fresh.term(),
        "current_own_mq_index": fresh.read_index(),
        "current_own_mq_schema4_succeeded": true,
        "root_exists": root.exists(),
        "constructor_error": error,
        "elapsed_ms": started.elapsed().as_millis(),
        "classification": if error.as_deref().is_some_and(|e| e.contains("learner identity must be actually registered before factory creation")) {
            "STALE_SEED_MEMBER_LOOKUP_REJECTED_BEFORE_CURRENT_MQ"
        } else if error.is_some() {
            "OTHER_CONSTRUCTOR_ERROR_NOT_A_STALE_SEED_WITNESS"
        } else { "CURRENT_AUTHORITY_DISCOVERED" },
        "not_explanation_of_prior_process_exit": true,
        "data_serving": false,
    });
    std::fs::write(
        f.root.path().join("stale-seed-constructor-result.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    if let Some(evidence) = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT") {
        let evidence = std::path::PathBuf::from(evidence).join("native-stale-seed-constructor");
        std::fs::create_dir_all(&evidence).unwrap();
        // Sanitized diagnostic only: no TLS files, databases or authority DTOs.
        std::fs::write(
            evidence.join("result.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
    }
    eprintln!("NATIVE_STALE_SEED_CONSTRUCTOR {report}");
    let succeeded = result.is_ok();
    if let Ok(mut owner) = result {
        let status = owner.refresh(&token).unwrap();
        assert_eq!(status.node_id, node(5));
        assert!(!status.data_serving);
        assert!(!status.birth_bound);
        assert!(status.metadata_store_id.is_none());
        assert_eq!(status.metadata_role, MetadataMemberRole::Learner);
        assert!(!status.namespace_ids.is_empty());
        owner.stop(&token).unwrap();
    } else {
        assert!(
            !root.exists(),
            "pre-root discovery must not leave a factory"
        );
    }
    old_control.stop(&token).unwrap();
    fifth.stop_owned_metadata(&token).unwrap();
    fifth.shutdown().unwrap();
    drop(current_store);
    f.shutdown();
    assert!(succeeded, "fresh constructor must discover current registered authority beyond stale seed C; actual constructor error: {error:?}");
}
