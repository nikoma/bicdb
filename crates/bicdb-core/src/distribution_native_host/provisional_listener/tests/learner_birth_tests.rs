//! Genuine four-voter metadata/MQ + fifth unassigned factory; real mTLS,
//! one physical host. No enrollment, metadata vote, range floor or data grant.
use super::*;
use crate::distribution_native_host::{
    NativeUnassignedLearnerFactory, NativeUnassignedLearnerHost,
};
use std::time::Instant;
#[test]
fn native_unassigned_learner_birth_tls_live_scope_cancel_and_permanent_provisional_refusal() {
    let PlannedNetwork {
        mut root,
        plan,
        networks,
        ports,
    } = planned_network_with_spare(4, 1);
    root.disable_cleanup(true);
    eprintln!("NATIVE_LEARNER_BIRTH_TLS_ROOT {}", root.path().display());
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
    let source_cancel = Arc::new(AtomicBool::new(false));
    let source_parent = CancellationToken::new(Arc::clone(&source_cancel), None);
    let source_business = Uuid::now_v7();
    let source_mq = transport
        .acquire_host_metadata_observation(
            &node(0),
            source_business,
            Duration::from_secs(5),
            &source_parent,
        )
        .unwrap();
    let bundle = seeds[0].database.read().cluster_schema_bundle().unwrap();
    let factory = Arc::new(
        NativeUnassignedLearnerFactory::create(
            &root.path().join("learner-observer"),
            db_config(),
            &source_mq,
            source_business,
            &networks[4],
            &bundle,
            &token,
        )
        .unwrap(),
    );
    let uid = factory
        .database
        .read()
        .collections()
        .into_iter()
        .find(|m| m.name == "items")
        .unwrap()
        .local_namespace_id;
    let mut learner = NativeUnassignedLearnerHost::start(
        Arc::clone(&factory),
        source_mq,
        source_business,
        networks[4].clone(),
        &token,
    )
    .unwrap();
    let service = learner.test_service();
    let caller_cancel = Arc::new(AtomicBool::new(false));
    let caller_parent = CancellationToken::new(Arc::clone(&caller_cancel), None);
    let business = Uuid::now_v7();
    let caller_mq = launches[0]
        .transport
        .acquire_host_metadata_observation(
            &node(0),
            business,
            Duration::from_secs(5),
            &caller_parent,
        )
        .unwrap();
    let observed = launches[0]
        .transport
        .acquire_learner_birth(&node(4), caller_mq, business, &token)
        .unwrap();
    let birth = observed.birth().unwrap().clone();
    assert_eq!(observed.node().unwrap(), &node(4));
    assert_eq!(observed.schema_sha256().unwrap(), bundle.fingerprint.sha256);
    assert_eq!(birth.node_incarnation(), 1);
    assert!(
        birth.writer_enforcement_version() == 1
            && birth.abort_protocol_version() == 1
            && factory.spec.initial_ranges.is_empty()
    );
    assert!(
        launches[0].metadata.lock().committed_topology().nodes[&node(4)]
            .native_store_birth_v1
            .is_none(),
        "observation cannot publish metadata enrollment"
    );
    caller_cancel.store(true, Ordering::SeqCst);
    assert!(
        observed.require_live().is_err(),
        "original caller MQ cancellation cannot be relabeled"
    );

    let business = Uuid::now_v7();
    let fresh_caller = launches[0]
        .transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
        .unwrap();
    source_cancel.store(true, Ordering::SeqCst);
    {
        let _service = service.lock();
        let _database = factory.database.write();
        let started = Instant::now();
        let error = launches[0]
            .transport
            .acquire_learner_birth(&node(4), fresh_caller, business, &token)
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains(&BicDbError::QueryCanceled.to_string()),
            "{error}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "expired source proof must refuse before latches"
        );
    }
    let renewed_business = Uuid::now_v7();
    let renewed = transport
        .acquire_host_metadata_observation(
            &node(0),
            renewed_business,
            Duration::from_secs(5),
            &token,
        )
        .unwrap();
    learner
        .refresh_metadata(renewed, renewed_business, &token)
        .unwrap();
    let business = Uuid::now_v7();
    let caller_mq = launches[0]
        .transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
        .unwrap();
    let current = launches[0]
        .transport
        .acquire_learner_birth(&node(4), caller_mq, business, &token)
        .unwrap();
    assert_eq!(current.birth().unwrap(), &birth);
    assert!(
        observed.require_live().is_err(),
        "renewal is an independent proof, never revives original request"
    );
    assert_eq!(
        factory
            .database
            .read()
            .collections()
            .into_iter()
            .find(|m| m.name == "items")
            .unwrap()
            .local_namespace_id,
        uid
    );
    factory
        .database
        .read()
        .require_native_metadata_uninitialized()
        .unwrap();
    let range = registered.ranges.values().next().unwrap();
    assert_eq!(
        factory
            .database
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        None,
        "an unassigned factory has no invented R0 range floor"
    );
    assert!(!factory.database.read().commit_admission_ready());

    let mut stopped_provider_listener = learner.test_take_listener();
    drop(learner);
    {
        let _service = service.lock();
        let _database = factory.database.write();
        let error = launches[0]
            .transport
            .application_event(
                &node(4),
                crate::ClusterEventRequest {
                    range_id: range.id,
                    range_epoch: range.epoch,
                    operation: crate::ClusterEventOperation::Scan {
                        namespace: "items".into(),
                        after: None,
                        limit: 1,
                    },
                },
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("provisional learner listener"),
            "{error}"
        );
    }
    let business = Uuid::now_v7();
    let caller_mq = launches[0]
        .transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
        .unwrap();
    let error = launches[0]
        .transport
        .acquire_learner_birth(&node(4), caller_mq, business, &token)
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("factory is not active"),
        "{error}"
    );
    stopped_provider_listener
        .stop_and_drain(&token.child_with_deadline(Instant::now() + Duration::from_secs(5)))
        .unwrap();
    for listener in &mut listeners {
        listener.stop_and_drain(&token).unwrap();
    }
}
