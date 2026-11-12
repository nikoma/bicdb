//! Actual independent pinned TLS listeners and native paged receipts. Initial
//! consensus permission is NOT data readiness, ordinary reads or HA qualification.
use super::*;
use crate::distribution_consensus::{MetadataConsensusStore, MetadataVoteRequest};
use crate::distribution_native_host::{
    NativeInitialPreparationRequest, NativeInitialPreparationStage,
};
fn retained_fixture() -> Fixture {
    let mut f = Fixture::new();
    f._root.disable_cleanup(true);
    eprintln!(
        "NATIVE_INITIAL_PREPARATION_ROOT {}",
        f._root.path().display()
    );
    f
}
fn record(host: &NativeProvisionalHost) -> crate::db::NativeInitialPreparationRecord {
    host.seed
        .database
        .read()
        .read_native_initial_preparation(
            &host.seed.spec.cluster_id,
            host.seed.spec.bootstrap_id,
            &host.seed.spec.node_id,
            host.seed.spec.member_incarnation,
        )
        .unwrap()
        .unwrap()
}
fn vote(f: &Fixture) -> MetadataVoteRequest {
    MetadataVoteRequest {
        cluster_id: f.plan.topology.cluster_id.clone(),
        term: 1,
        candidate_id: node(0),
        last_log_index: 0,
        last_log_term: 0,
    }
}
fn assert_no_data(host: &NativeProvisionalHost) {
    let db = host.seed.database.read();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "items",
        Record::new("initial-barrier-is-not-data-admission"),
    )
    .unwrap();
    assert!(tx.commit().is_err());
    assert!(db
        .get("items", "initial-barrier-is-not-data-admission")
        .unwrap()
        .is_none());
}
fn metadata_client_for_configuration(
    f: &Fixture,
    configuration: &NativeInitialMetadataConfiguration,
) -> TcpClusterRelocationTransport {
    TcpClusterRelocationTransport::new(
        configuration.topology.cluster_id.clone(),
        node(0),
        Arc::new(RwLock::new(configuration.topology.clone())),
        f.networks[0].clone(),
    )
    .unwrap()
}

#[test]
fn native_initial_preparation_tls_sequential_launch_uses_historical_same_c_after_first_vote() {
    let mut f = retained_fixture();
    let token = CancellationToken::uncancelable();
    let c = f.hosts[0].observe_initial_configuration(&token).unwrap();
    // The provisional client intentionally has only the unenrolled plan.
    // Countable metadata RPCs use the actual birth-bound initial C instead.
    let metadata_client = TcpClusterRelocationTransport::new(
        c.topology.cluster_id.clone(),
        node(0),
        Arc::new(RwLock::new(c.topology.clone())),
        f.networks[0].clone(),
    )
    .unwrap();
    let original_service = Arc::clone(&f.hosts[0]._service);
    let original_store = original_service
        .lock()
        .native_initial_listener_store_open_id();
    let permit = f.hosts[0]
        .prepare_common_initial_configuration(&c, &token)
        .unwrap();
    for host in &f.hosts {
        assert_eq!(
            record(host).stage(),
            NativeInitialPreparationStage::PreparedNoVote
        );
        let slot = host.seed.prepared_metadata.lock();
        let metadata = slot.as_ref().unwrap();
        assert_eq!(metadata.status().current_term, 0);
        assert!(metadata.status().voted_for.is_none());
        assert_eq!(metadata.committed_topology(), &c.topology);
        drop(slot);
        assert_no_data(host);
    }
    assert!(metadata_client
        .request_metadata_vote(&node(1), vote(&f))
        .unwrap_err()
        .to_string()
        .contains("provisional factory listener"));
    let launch0 = f.hosts[0]
        .finish_initial_consensus(&permit, &token)
        .unwrap();
    assert!(Arc::ptr_eq(&original_service, &launch0.service));
    assert_eq!(
        launch0
            .service
            .lock()
            .native_initial_listener_store_open_id(),
        original_store
    );
    let mut listeners = vec![launch0
        .start_metadata_listener(f.networks[0].clone(), &token)
        .unwrap()];
    let request = launch0.metadata.lock().start_election().unwrap();
    assert_eq!(request.term, 1);
    // Later callers CANNOT ask n0 for unregistered birth facts or a fresh T0.
    assert!(f.offer(0, &token).is_err());
    let mut launches = vec![launch0];
    for index in 1..3 {
        let observed = f.hosts[index]
            .observe_initial_configuration(&token)
            .unwrap();
        assert_eq!(observed.topology, c.topology);
        let permit = f.hosts[index]
            .prepare_common_initial_configuration(&observed, &token)
            .unwrap();
        let launch = f.hosts[index]
            .finish_initial_consensus(&permit, &token)
            .unwrap();
        listeners.push(
            launch
                .start_metadata_listener(f.networks[index].clone(), &token)
                .unwrap(),
        );
        launches.push(launch);
    }
    let mut voters = BTreeSet::from([node(0)]);
    for index in 1..3 {
        let reply = metadata_client
            .request_metadata_vote(&node(index), request.clone())
            .unwrap();
        assert!(reply.vote_granted);
        voters.insert(reply.voter_id);
    }
    launches[0].metadata.lock().become_leader(&voters).unwrap();
    for _ in 0..3 {
        for index in 1..3 {
            let request = launches[0]
                .metadata
                .lock()
                .append_request_from(1, 16)
                .unwrap();
            let reply = metadata_client
                .append_metadata(&node(index), request)
                .unwrap();
            launches[0]
                .metadata
                .lock()
                .record_append_response(&node(index), &reply)
                .unwrap();
        }
    }
    assert!(launches[0].metadata.lock().status().commit_index > 0);
    // Real later metadata commit (load-only topology change), not a forged
    // generation or a claim that the historical C is current authority.
    let mut next = launches[0].metadata.lock().committed_topology().clone();
    next.generation += 1;
    next.ranges.values_mut().next().unwrap().approximate_bytes += 17;
    launches[0]
        .metadata
        .lock()
        .propose_topology(next.clone())
        .unwrap();
    for _ in 0..3 {
        for index in 1..3 {
            let request = launches[0]
                .metadata
                .lock()
                .append_request_from(1, 16)
                .unwrap();
            let reply = metadata_client
                .append_metadata(&node(index), request)
                .unwrap();
            launches[0]
                .metadata
                .lock()
                .record_append_response(&node(index), &reply)
                .unwrap();
        }
    }
    assert_eq!(launches[0].metadata.lock().committed_topology(), &next);
    let historical = f.hosts[2]
        .transport
        .acquire_initial_preparation(&node(0), &f.plan, Some(c.topology.clone()), &token)
        .unwrap();
    assert_eq!(
        historical.stage(),
        NativeInitialPreparationStage::ConsensusExposedHistorical
    );
    assert_eq!(historical.configuration_data(), &c.topology);
    for host in &f.hosts {
        assert_eq!(
            record(host).stage(),
            NativeInitialPreparationStage::ConsensusExposedHistorical
        );
        assert_no_data(host);
    }
    let range = c.topology.ranges.values().next().unwrap();
    let scan = crate::ClusterEventRequest {
        range_id: range.id,
        range_epoch: range.epoch,
        operation: crate::ClusterEventOperation::Scan {
            namespace: "unqualified".into(),
            after: None,
            limit: 1,
        },
    };
    assert!(f.hosts[0]
        .transport
        .application_event(&node(1), scan)
        .unwrap_err()
        .to_string()
        .contains("metadata endpoints only"));
    for listener in &mut listeners {
        listener.stop_and_drain(&token).unwrap();
    }
}
#[test]
fn native_initial_preparation_tls_foreign_c_and_incomplete_barrier_never_expose_votes() {
    let f = retained_fixture();
    let token = CancellationToken::uncancelable();
    let c = f.hosts[0].observe_initial_configuration(&token).unwrap();
    let mut foreign = c.topology.clone();
    let range = foreign.ranges.values_mut().next().unwrap();
    range.leader = range
        .replicas
        .iter()
        .find(|r| r.node_id != range.leader)
        .unwrap()
        .node_id
        .clone();
    assert!(f.hosts[0]
        .transport
        .acquire_initial_preparation(&node(1), &f.plan, Some(foreign), &token)
        .is_err());
    f.assert_no_metadata();
    let one = f.hosts[0]
        .transport
        .acquire_initial_preparation(&node(1), &f.plan, Some(c.topology.clone()), &token)
        .unwrap();
    assert!(
        crate::distribution_native_host::NativeInitialConsensusPermit::from_observations(
            &f.hosts[0].seed,
            &c,
            vec![one],
            &token
        )
        .is_err()
    );
    let saved = serde_json::to_vec(&record(&f.hosts[1])).unwrap();
    let mut changed = c.topology.clone();
    changed.nodes.get_mut(&node(0)).unwrap().address = "127.0.0.1:1".into();
    assert!(f.hosts[0]
        .transport
        .acquire_initial_preparation(&node(1), &f.plan, Some(changed), &token)
        .is_err());
    assert_eq!(serde_json::to_vec(&record(&f.hosts[1])).unwrap(), saved);
    assert!(metadata_client_for_configuration(&f, &c)
        .request_metadata_vote(&node(1), vote(&f))
        .is_err());
    assert_eq!(
        f.hosts[1]
            .seed
            .prepared_metadata
            .lock()
            .as_ref()
            .unwrap()
            .status()
            .current_term,
        0
    );
    assert_no_data(&f.hosts[1]);
}
#[test]
fn native_initial_preparation_tls_original_cancel_cannot_relabel_accepted_fact() {
    let f = retained_fixture();
    let parent = CancellationToken::uncancelable();
    let c = f.hosts[0].observe_initial_configuration(&parent).unwrap();
    let short = parent.child();
    let request = NativeInitialPreparationRequest {
        version: 1,
        challenge: Uuid::now_v7(),
        requester: node(0),
        requester_incarnation: 1,
        plan_sha256: f.plan.checksum().unwrap(),
        configuration: Some(c.topology.clone()),
        budget_ms: 5000,
    };
    let sealed = f.hosts[0]
        .transport
        .request_initial_preparation_reply(&node(1), request, &short)
        .unwrap();
    short.cancel();
    assert!(
        crate::distribution_native_host::NativeInitialPreparationObservation::from_transport(
            sealed, &f.plan
        )
        .is_err()
    );
    // The durable acceptance survives; only a NEW actual exchange can observe
    // it under a new operation. It is not a renewed cached reply/role lease.
    f.hosts[0]
        .transport
        .acquire_initial_preparation(&node(1), &f.plan, Some(c.topology.clone()), &parent)
        .unwrap();
    assert_eq!(
        record(&f.hosts[1]).stage(),
        NativeInitialPreparationStage::PreparedNoVote
    );
    assert!(metadata_client_for_configuration(&f, &c)
        .request_metadata_vote(&node(1), vote(&f))
        .is_err());
}
#[test]
fn native_initial_preparation_tls_drain_deadline_retains_stopping_owner_and_same_store() {
    let mut f = retained_fixture();
    let parent = CancellationToken::uncancelable();
    let c = f.hosts[0].observe_initial_configuration(&parent).unwrap();
    let permit = f.hosts[0]
        .prepare_common_initial_configuration(&c, &parent)
        .unwrap();
    let store = f.hosts[0]
        ._service
        .lock()
        .native_initial_listener_store_open_id();
    let stalled = std::net::TcpStream::connect(&f.plan.topology.nodes[&node(0)].address).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while f.hosts[0].listener.as_ref().unwrap().active_connections() == 0 {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    let short = parent.child_with_deadline(std::time::Instant::now() + Duration::from_millis(10));
    assert!(f.hosts[0]
        .finish_initial_consensus(&permit, &short)
        .is_err());
    assert!(f.hosts[0].listener.is_some());
    assert_eq!(
        record(&f.hosts[0]).stage(),
        NativeInitialPreparationStage::PreparedNoVote
    );
    drop(stalled);
    f.hosts[0]
        .restart_stopped_initial_listener(f.networks[0].clone(), &parent)
        .unwrap();
    let fresh = f.hosts[0]
        .prepare_common_initial_configuration(&c, &parent)
        .unwrap();
    let launch = f.hosts[0]
        .finish_initial_consensus(&fresh, &parent)
        .unwrap();
    assert_eq!(
        launch
            .service
            .lock()
            .native_initial_listener_store_open_id(),
        store
    );
    assert_eq!(launch.metadata.lock().status().current_term, 0);
}

#[test]
fn native_initial_preparation_tls_prepared_cold_copy_reopens_same_store_without_birth_fallback() {
    let mut f = retained_fixture();
    let token = CancellationToken::uncancelable();
    let c = f.hosts[0].observe_initial_configuration(&token).unwrap();
    let _permit = f.hosts[0]
        .prepare_common_initial_configuration(&c, &token)
        .unwrap();
    let root = f.hosts[1].seed.root.clone();
    let before = record(&f.hosts[1]);
    let old_open = f.hosts[1].seed.database.read().incarnation_id().to_owned();
    let before_native = f.hosts[1]
        .seed
        .spec
        .initial_ranges
        .iter()
        .map(|range| {
            let point = f.hosts[1]
                .seed
                .database
                .read()
                .native_range_prefix_observation(range.id)
                .unwrap();
            assert_eq!(point.native_applied_index, Some(0));
            assert!(!point.invalidated);
            (range.id, point.native_receipt_sha256)
        })
        .collect::<Vec<_>>();
    let metadata = root
        .join(crate::distribution_native_host::initial_preparation::INITIAL_METADATA_DIRECTORY)
        .join(crate::distribution_consensus::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
    let saved = std::fs::read(&metadata).unwrap();
    let mut old = f.hosts.remove(1);
    old.listener
        .as_mut()
        .unwrap()
        .stop_and_drain(&token)
        .unwrap();
    old.shutdown().unwrap();
    let mut copy = crate::db::ActualNativeRf3Fixture::copy_stopped_recovery_tree(&root).unwrap();
    copy.disable_cleanup(true);
    eprintln!(
        "NATIVE_INITIAL_PREPARATION_COLD_COPY {}",
        copy.path().display()
    );
    let seed = Arc::new(
        NativeProvisionalNode::open_initial_preparation(
            copy.path(),
            db_config(),
            f.plan.clone(),
            &node(1),
            &token,
        )
        .unwrap(),
    );
    assert_ne!(seed.database.read().incarnation_id(), old_open);
    for (range, original_receipt) in before_native {
        let point = seed
            .database
            .read()
            .native_range_prefix_observation(range)
            .unwrap();
        assert_eq!(point.native_applied_index, Some(0));
        assert!(!point.invalidated && point.pending_command_index.is_none());
        assert_eq!(
            point.native_receipt_sha256, original_receipt,
            "cold validation must retain actual original genesis, never reseal it"
        );
    }
    let host =
        NativeProvisionalHost::start(Arc::clone(&seed), f.networks[1].clone(), &token).unwrap();
    assert!(f.hosts[0]
        .transport
        .acquire_initial_birth_offer(&node(1), &f.plan, &token)
        .is_err());
    f.hosts[0]
        .transport
        .acquire_initial_preparation(&node(1), &f.plan, Some(c.topology.clone()), &token)
        .unwrap();
    let current = record(&host);
    assert_eq!(current.peer_fact().store_id, before.peer_fact().store_id);
    assert_eq!(
        current.peer_fact().acceptance_sha256,
        before.peer_fact().acceptance_sha256
    );
    assert_eq!(
        current.stage(),
        NativeInitialPreparationStage::PreparedNoVote
    );
    assert_eq!(
        seed.prepared_metadata
            .lock()
            .as_ref()
            .unwrap()
            .status()
            .current_term,
        0
    );
    assert_eq!(
        std::fs::read(&metadata).unwrap(),
        saved,
        "original stopped metadata never reopened"
    );
    let copied_metadata = copy
        .path()
        .join(crate::distribution_native_host::initial_preparation::INITIAL_METADATA_DIRECTORY)
        .join(crate::distribution_consensus::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
    assert_eq!(std::fs::read(copied_metadata).unwrap(), saved);
    host.shutdown().unwrap();
}

#[test]
fn native_initial_preparation_exposed_cold_copy_never_reenters_prevote_or_resets_term() {
    let mut f = retained_fixture();
    let token = CancellationToken::uncancelable();
    let c = f.hosts[0].observe_initial_configuration(&token).unwrap();
    let permit = f.hosts[0]
        .prepare_common_initial_configuration(&c, &token)
        .unwrap();
    let root = f.hosts[0].seed.root.clone();
    let spec = f.hosts[0].seed.spec.clone();
    let launch = f.hosts[0]
        .finish_initial_consensus(&permit, &token)
        .unwrap();
    launch.metadata.lock().start_election().unwrap();
    assert_eq!(launch.metadata.lock().status().current_term, 1);
    let metadata_path = root
        .join(crate::distribution_native_host::initial_preparation::INITIAL_METADATA_DIRECTORY)
        .join(crate::distribution_consensus::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
    let bytes = std::fs::read(&metadata_path).unwrap();
    drop(launch);
    f.hosts.remove(0).shutdown().unwrap();
    let mut copy = crate::db::ActualNativeRf3Fixture::copy_stopped_recovery_tree(&root).unwrap();
    copy.disable_cleanup(true);
    eprintln!("NATIVE_INITIAL_EXPOSED_COLD_COPY {}", copy.path().display());
    let error = NativeProvisionalNode::open_initial_preparation(
        copy.path(),
        db_config(),
        f.plan.clone(),
        &node(0),
        &token,
    )
    .err()
    .unwrap();
    assert!(
        error.to_string().contains("MAY HAVE BEEN EXPOSED"),
        "{error}"
    );
    // Ordinary ACTUAL native-owned Raft recovery preserves the durable vote;
    // this is not a new initial grant, election quorum or data readiness proof.
    let (db, _) = BicDb::open_range_image_database(copy.path(), db_config(), &spec).unwrap();
    let db = Arc::new(RwLock::new(db));
    let metadata = MetadataConsensusStore::reopen_native_owned(
        copy.path()
            .join(crate::distribution_native_host::initial_preparation::INITIAL_METADATA_DIRECTORY),
        Arc::clone(&db),
        node(0),
        c.topology.clone(),
        &token,
    )
    .unwrap();
    assert_eq!(metadata.status().current_term, 1);
    assert_eq!(metadata.status().voted_for, Some(node(0)));
    assert_eq!(
        std::fs::read(&metadata_path).unwrap(),
        bytes,
        "original metadata remains frozen"
    );
}

#[test]
fn native_initial_preparation_tls_protected_proposal_and_both_metadata_cuts_resume_exact_c_on_cold_copy(
) {
    for after_state in [false, true] {
        let mut f = retained_fixture();
        let token = CancellationToken::uncancelable();
        let c = f.hosts[0].observe_initial_configuration(&token).unwrap();
        let root = f.hosts[1].seed.root.clone();
        let error = f.hosts[1]
            .seed
            .test_initial_metadata_preparation_cut(&c, after_state, &token)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("after actual durable publication"));
        let identity = {
            let seed = &f.hosts[1].seed;
            seed.database
                .read()
                .native_metadata_initialization_record(
                    &seed.spec.cluster_id,
                    seed.spec.bootstrap_id,
                    &seed.spec.node_id,
                    seed.spec.member_incarnation,
                )
                .unwrap()
                .unwrap()
        };
        assert!(!identity.registered());
        assert!(f.hosts[1]
            .seed
            .database
            .read()
            .read_native_initial_preparation(
                &f.plan.topology.cluster_id,
                f.plan.topology.range_image_bootstrap_id.unwrap(),
                &node(1),
                1
            )
            .unwrap()
            .is_none());
        // A protected PROPOSAL is CONFIG data only. There is no Accepted
        // observation available for the all-member barrier at this cut.
        assert!(f.offer(1, &token).is_err());
        let config_only = f.hosts[0]
            .transport
            .discover_initial_configuration(&node(1), &f.plan, &token)
            .unwrap();
        let discovered = NativeInitialMetadataConfiguration::from_configuration_data_observation(
            &f.hosts[0].seed,
            config_only,
        )
        .unwrap();
        assert_eq!(discovered.topology, c.topology);
        assert!(metadata_client_for_configuration(&f, &c)
            .request_metadata_vote(&node(1), vote(&f))
            .is_err());
        let mut old = f.hosts.remove(1);
        old.listener
            .as_mut()
            .unwrap()
            .stop_and_drain(&token)
            .unwrap();
        old.shutdown().unwrap();
        let mut copy =
            crate::db::ActualNativeRf3Fixture::copy_stopped_recovery_tree(&root).unwrap();
        copy.disable_cleanup(true);
        eprintln!(
            "NATIVE_INITIAL_PREPARE_CUT_COLD_COPY {}",
            copy.path().display()
        );
        let seed = Arc::new(
            NativeProvisionalNode::open_initial_preparation(
                copy.path(),
                db_config(),
                f.plan.clone(),
                &node(1),
                &token,
            )
            .unwrap(),
        );
        let host =
            NativeProvisionalHost::start(Arc::clone(&seed), f.networks[1].clone(), &token).unwrap();
        f.hosts[0]
            .transport
            .acquire_initial_preparation(&node(1), &f.plan, Some(c.topology.clone()), &token)
            .unwrap();
        let accepted = record(&host);
        assert_eq!(accepted.peer_fact().store_id, identity.store_id());
        assert_eq!(accepted.configuration(), &c.topology);
        let metadata = seed.prepared_metadata.lock();
        let status = metadata.as_ref().unwrap().status();
        assert_eq!(status.current_term, 0);
        assert!(status.voted_for.is_none());
        drop(metadata);
        assert!(metadata_client_for_configuration(&f, &c)
            .request_metadata_vote(&node(1), vote(&f))
            .is_err());
        host.shutdown().unwrap();
    }
}
