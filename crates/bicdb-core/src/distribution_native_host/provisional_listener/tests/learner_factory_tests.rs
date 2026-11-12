//! Four actual native metadata stores/listeners and a fifth TLS requester on
//! one host. Factory admission only: no relocation, Ready or user ACK claim.
use super::*;
use crate::distribution_native_host::NativeUnassignedLearnerFactory;

#[test]
fn native_unassigned_fifth_factory_actual_host_mq_create_reopen_and_proposal() {
    let PlannedNetwork {
        mut root,
        plan,
        networks,
        ports,
    } = planned_network_with_spare(4, 1);
    root.disable_cleanup(true);
    eprintln!("NATIVE_UNASSIGNED_FIFTH_ROOT {}", root.path().display());
    let token = CancellationToken::uncancelable();
    let fifth = ClusterNode::new(
        node(4),
        ports[4].local_addr().unwrap().to_string(),
        1,
        1 << 30,
        10,
    )
    .unwrap()
    .as_metadata_learner()
    .with_tls_certificate_sha256(
        crate::replication_transport::replication_certificate_sha256(
            &networks[4].tls.as_ref().unwrap().cert_path,
        )
        .unwrap(),
    )
    .unwrap();
    let seeds = (0..4)
        .map(|i| {
            Arc::new(
                NativeProvisionalNode::create(
                    &root.path().join(format!("source-db{i}")),
                    db_config(),
                    plan.clone(),
                    &node(i),
                    &token,
                )
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    drop(ports);
    let mut hosts = seeds
        .iter()
        .enumerate()
        .map(|(i, seed)| {
            NativeProvisionalHost::start(Arc::clone(seed), networks[i].clone(), &token).unwrap()
        })
        .collect::<Vec<_>>();
    let c = hosts[0].observe_initial_configuration(&token).unwrap();
    let permits = hosts
        .iter()
        .map(|host| {
            host.prepare_common_initial_configuration(&c, &token)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let launches = hosts
        .iter_mut()
        .zip(&permits)
        .map(|(host, permit)| host.finish_initial_consensus(permit, &token).unwrap())
        .collect::<Vec<_>>();
    let mut listeners = launches
        .iter()
        .enumerate()
        .map(|(i, launch)| {
            launch
                .start_metadata_listener(networks[i].clone(), &token)
                .unwrap()
        })
        .collect::<Vec<_>>();
    launches[0].elect_initial_metadata(&token).unwrap();
    let mut registered = launches[0].metadata.lock().committed_topology().clone();
    let mut fifth = fifth;
    fifth.labels = registered.nodes[&node(0)].labels.clone();
    registered.nodes.insert(node(4), fifth);
    registered.generation += 1;
    launches[0]
        .metadata
        .lock()
        .propose_topology(registered.clone())
        .unwrap();
    launches[0].heartbeat_initial_metadata(&token).unwrap();
    for launch in &launches {
        assert_eq!(launch.metadata.lock().committed_topology(), &registered);
        assert_eq!(*launch.topology.read(), registered);
    }
    // The requester's identity is known from an ACTUAL committed admission;
    // no protected store or metadata vote exists for it yet.
    let transport = TcpClusterRelocationTransport::new(
        registered.cluster_id.clone(),
        node(4),
        Arc::new(RwLock::new(registered.clone())),
        networks[4].clone(),
    )
    .unwrap();
    let business = Uuid::now_v7();
    let observed = transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
        .unwrap();
    let bundle = seeds[0].database.read().cluster_schema_bundle().unwrap();
    let factory_root = root.path().join("fresh-unassigned-fifth");
    assert!(NativeUnassignedLearnerFactory::create(
        &root.path().join("wrong-local-pin"),
        db_config(),
        &observed,
        business,
        &networks[0],
        &bundle,
        &token,
    )
    .is_err());
    assert!(!root.path().join("wrong-local-pin").exists());
    assert!(NativeUnassignedLearnerFactory::create(
        &root.path().join("wrong-business"),
        db_config(),
        &observed,
        Uuid::now_v7(),
        &networks[4],
        &bundle,
        &token,
    )
    .is_err());
    assert!(!root.path().join("wrong-business").exists());
    let factory = NativeUnassignedLearnerFactory::create(
        &factory_root,
        db_config(),
        &observed,
        business,
        &networks[4],
        &bundle,
        &token,
    )
    .unwrap();
    assert!(factory.spec.initial_ranges.is_empty());
    assert!(factory
        .database
        .read()
        .scan_collection("items")
        .unwrap()
        .is_empty());
    let proposal = factory
        .enrollment_proposal(&observed, business, &networks[4])
        .unwrap();
    let birth = proposal.nodes[&node(4)]
        .native_store_birth_v1
        .clone()
        .unwrap();
    assert_eq!(
        proposal.nodes[&node(4)].metadata_role,
        MetadataMemberRole::Learner
    );
    assert!(proposal
        .ranges
        .values()
        .all(|range| range.replicas.iter().all(|r| r.node_id != node(4))));
    // A configuration proposal does NOT publish enrollment, a vote or R0.
    assert!(
        launches[0].metadata.lock().committed_topology().nodes[&node(4)]
            .native_store_birth_v1
            .is_none()
    );
    factory
        .database
        .read()
        .require_native_metadata_uninitialized()
        .unwrap();
    let uid = factory
        .database
        .read()
        .collections()
        .into_iter()
        .find(|m| m.name == "items")
        .unwrap()
        .local_namespace_id;
    assert!(uid.is_some());
    let database = factory.database.read();
    let mut tx = database.begin_transaction().unwrap();
    tx.insert("items", Record::new("must-not-ack-from-learner-factory"))
        .unwrap();
    assert!(tx.commit().is_err());
    drop(tx);
    drop(database);
    drop(factory);

    // Independent canceled caller must refuse BEFORE touching even a missing
    // root. Exact error and unchanged absence distinguish it from I/O failure.
    let canceled = CancellationToken::uncancelable().child();
    canceled.cancel();
    let missing = root.path().join("canceled-reopen-missing");
    let error = NativeUnassignedLearnerFactory::reopen_uninitialized(
        &missing,
        db_config(),
        &observed,
        business,
        &networks[4],
        &canceled,
    )
    .err()
    .unwrap();
    assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
    assert!(!missing.exists());
    let fresh_business = Uuid::now_v7();
    let fresh = transport
        .acquire_host_metadata_observation(&node(0), fresh_business, Duration::from_secs(5), &token)
        .unwrap();
    let reopened = NativeUnassignedLearnerFactory::reopen_uninitialized(
        &factory_root,
        db_config(),
        &fresh,
        fresh_business,
        &networks[4],
        &token,
    )
    .unwrap();
    let proposal = reopened
        .enrollment_proposal(&fresh, fresh_business, &networks[4])
        .unwrap();
    assert_eq!(
        proposal.nodes[&node(4)].native_store_birth_v1.as_ref(),
        Some(&birth)
    );
    assert_eq!(
        reopened
            .database
            .read()
            .collections()
            .into_iter()
            .find(|m| m.name == "items")
            .unwrap()
            .local_namespace_id,
        uid
    );
    let canceled_original = Arc::new(AtomicBool::new(false));
    let original_parent = CancellationToken::new(Arc::clone(&canceled_original), None);
    let original_business = Uuid::now_v7();
    let original = transport
        .acquire_host_metadata_observation(
            &node(0),
            original_business,
            Duration::from_secs(5),
            &original_parent,
        )
        .unwrap();
    canceled_original.store(true, Ordering::SeqCst);
    assert!(reopened
        .enrollment_proposal(&original, original_business, &networks[4])
        .is_err());
    for listener in &mut listeners {
        listener.stop_and_drain(&token).unwrap();
    }
}
