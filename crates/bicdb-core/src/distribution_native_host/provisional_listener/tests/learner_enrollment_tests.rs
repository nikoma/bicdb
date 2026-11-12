//! Actual four-voter mTLS metadata and a fifth permanently provisional factory.
//! Binding an already registered identity does not create its owned MStore,
//! grant a vote/data placement, or implement automatic joining.
use super::*;
use crate::distribution_native_host::{
    NativeLearnerBirthObservation, NativeUnassignedLearnerFactory, NativeUnassignedLearnerHost,
};

struct EnrollmentFixture {
    listeners: Vec<ClusterDataServerHandle>,
    launches: Vec<NativeInitialConsensusLaunch>,
    seeds: Vec<Arc<NativeProvisionalNode>>,
    networks: Vec<ClusterNetworkTransportConfig>,
    learner_transport: TcpClusterRelocationTransport,
    root: tempfile::TempDir,
    next_learner_address: Option<String>,
}
impl EnrollmentFixture {
    fn new() -> Self {
        Self::with_spares(1)
    }
    fn with_spares(spares: usize) -> Self {
        assert!((1..=2).contains(&spares));
        let PlannedNetwork {
            mut root,
            plan,
            networks,
            ports,
        } = planned_network_with_spare(4, spares);
        let next_learner_address = ports.get(5).map(|p| p.local_addr().unwrap().to_string());
        root.disable_cleanup(true);
        eprintln!("NATIVE_LEARNER_ENROLLMENT_ROOT {}", root.path().display());
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
        let listeners = launches
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
        }
        let learner_transport = TcpClusterRelocationTransport::new(
            registered.cluster_id.clone(),
            node(4),
            Arc::new(RwLock::new(registered)),
            networks[4].clone(),
        )
        .unwrap();
        Self {
            listeners,
            launches,
            seeds,
            networks,
            learner_transport,
            root,
            next_learner_address,
        }
    }
    fn factory_only(&self, name: &str) -> Arc<NativeUnassignedLearnerFactory> {
        let token = CancellationToken::uncancelable();
        let business = Uuid::now_v7();
        let observed = self
            .learner_transport
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
            .unwrap();
        let bundle = self.seeds[0]
            .database
            .read()
            .cluster_schema_bundle()
            .unwrap();
        Arc::new(
            NativeUnassignedLearnerFactory::create(
                &self.root.path().join(name),
                db_config(),
                &observed,
                business,
                &self.networks[4],
                &bundle,
                &token,
            )
            .unwrap(),
        )
    }
    fn start_factory(
        &self,
        factory: Arc<NativeUnassignedLearnerFactory>,
    ) -> NativeUnassignedLearnerHost {
        let token = CancellationToken::uncancelable();
        let business = Uuid::now_v7();
        let observed = self
            .learner_transport
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
            .unwrap();
        NativeUnassignedLearnerHost::start(
            factory,
            observed,
            business,
            self.networks[4].clone(),
            &token,
        )
        .unwrap()
    }
    fn factory(
        &self,
        name: &str,
    ) -> (
        Arc<NativeUnassignedLearnerFactory>,
        NativeUnassignedLearnerHost,
    ) {
        let factory = self.factory_only(name);
        let host = self.start_factory(Arc::clone(&factory));
        (factory, host)
    }
    fn refresh_source_before_observation(&self, host: &NativeUnassignedLearnerHost) {
        let business = Uuid::now_v7();
        let token = CancellationToken::uncancelable();
        let current = self
            .learner_transport
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
            .unwrap();
        host.refresh_metadata(current, business, &token).unwrap();
    }
    fn observe(&self, token: &CancellationToken) -> NativeLearnerBirthObservation {
        let business = Uuid::now_v7();
        let observed = self.launches[0]
            .transport
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), token)
            .unwrap();
        self.launches[0]
            .transport
            .install_topology(observed.committed_topology(business).unwrap().clone())
            .unwrap();
        self.launches[0]
            .transport
            .acquire_learner_birth(&node(4), observed, business, token)
            .unwrap()
    }
    fn current(
        &self,
    ) -> (
        crate::distribution_consensus::HostMetadataReadObservation,
        Uuid,
    ) {
        let business = Uuid::now_v7();
        let observed = self.launches[0]
            .transport
            .acquire_host_metadata_observation(
                &node(0),
                business,
                Duration::from_secs(5),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        (observed, business)
    }
    fn commit(&self, mut topology: ClusterTopology) {
        topology.generation += 1;
        self.launches[0]
            .metadata
            .lock()
            .propose_topology(topology)
            .unwrap();
        self.launches[0]
            .heartbeat_initial_metadata(&CancellationToken::uncancelable())
            .unwrap();
    }
    fn reject_without_proposal(&self, fact: &NativeLearnerBirthObservation, expected: &str) {
        fact.require_live().unwrap_or_else(|error| panic!("semantic-negative precondition {expected}: original fact expired BEFORE current MQ: {error}"));
        let (current, business) = self.current();
        fact.require_live().unwrap_or_else(|error| panic!("semantic-negative precondition {expected}: original fact expired during current MQ: {error}"));
        eprintln!("NATIVE_LEARNER_SEMANTIC_NEGATIVE expected={expected} original_fact_live=true");
        let mut store = self.launches[0].metadata.lock();
        let before = store.status();
        let topology = store.committed_topology().clone();
        let error = store
            .propose_observed_native_learner_enrollment(
                fact,
                &current,
                business,
                &CancellationToken::uncancelable(),
            )
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(store.status().last_log_index, before.last_log_index);
        assert_eq!(store.status().commit_index, before.commit_index);
        assert_eq!(store.committed_topology(), &topology);
    }
    fn shutdown(mut self) {
        for listener in self.listeners.drain(..) {
            listener.join().unwrap();
        }
    }
}

#[test]
fn native_registered_learner_enrollment_tls_commits_exact_birth_and_preserves_identity_fences() {
    let f = EnrollmentFixture::new();
    let token = CancellationToken::uncancelable();
    // Complete BOTH genuine physical factories before capturing the held A
    // fact. Factory fsync time is not part of the semantic-negative schedule.
    let factory_b = f.factory_only("actual-factory-B");
    let (factory_a, host_a) = f.factory("actual-factory-A");
    f.refresh_source_before_observation(&host_a);
    let fact_a = f.observe(&token);
    let birth_a = fact_a.birth().unwrap().clone();
    host_a.shutdown().unwrap();
    // Both declarations arise from real independent physical factories. The
    // first observed body is never edited or turned into a fresh authority.
    let host_b = f.start_factory(Arc::clone(&factory_b));
    let uid_b = factory_b
        .database
        .read()
        .collections()
        .into_iter()
        .find(|m| m.name == "items")
        .unwrap()
        .local_namespace_id;
    let enrolled = f.launches[0]
        .enroll_registered_native_learner(&node(4), &token)
        .unwrap();
    assert_eq!(enrolled.node(), &node(4));
    assert_ne!(enrolled.birth(), &birth_a);
    assert!(enrolled.committed_index() > 0 && enrolled.metadata_term() > 0);
    let committed = f.launches[0].metadata.lock().committed_topology().clone();
    for launch in &f.launches {
        assert_eq!(launch.metadata.lock().committed_topology(), &committed);
        assert_eq!(*launch.topology.read(), committed);
    }
    assert_eq!(
        committed.nodes[&node(4)].native_store_birth_v1.as_ref(),
        Some(enrolled.birth())
    );
    assert_eq!(
        committed.nodes[&node(4)].metadata_role,
        MetadataMemberRole::Learner
    );
    assert!(committed
        .ranges
        .values()
        .all(|r| r.replicas.iter().all(|p| p.node_id != node(4))));
    factory_b
        .database
        .read()
        .require_native_metadata_uninitialized()
        .unwrap();
    assert!(factory_b.spec.initial_ranges.is_empty());
    assert!(!factory_b.database.read().commit_admission_ready());
    for range in committed.ranges.values() {
        assert_eq!(
            factory_b
                .database
                .read()
                .native_range_prefix_observation(range.id)
                .unwrap()
                .native_applied_index,
            None
        );
    }
    assert!(factory_b
        .database
        .read()
        .scan_collection("items")
        .unwrap()
        .is_empty());
    assert_eq!(
        factory_b
            .database
            .read()
            .collections()
            .into_iter()
            .find(|m| m.name == "items")
            .unwrap()
            .local_namespace_id,
        uid_b
    );
    f.reject_without_proposal(&fact_a, "birth changed");
    let before = f.launches[0].metadata.lock().status();
    let repeated = f.launches[0]
        .enroll_registered_native_learner(&node(4), &token)
        .unwrap();
    assert_eq!(repeated.birth(), enrolled.birth());
    assert_eq!(
        f.launches[0].metadata.lock().status().last_log_index,
        before.last_log_index
    );
    assert_eq!(
        f.launches[0].metadata.lock().committed_topology(),
        &committed
    );

    f.refresh_source_before_observation(&host_b);
    let fact_pins = f.observe(&token);
    let mut changed_pins = committed.clone();
    let genuine_other_pin = changed_pins.nodes[&node(3)].tls_certificate_sha256.clone();
    changed_pins
        .nodes
        .get_mut(&node(4))
        .unwrap()
        .tls_certificate_sha256 = genuine_other_pin;
    f.commit(changed_pins);
    f.reject_without_proposal(&fact_pins, "pins/schema/restore/birth changed");
    let mut restored = f.launches[0].metadata.lock().committed_topology().clone();
    restored
        .nodes
        .get_mut(&node(4))
        .unwrap()
        .tls_certificate_sha256 = committed.nodes[&node(4)].tls_certificate_sha256.clone();
    f.commit(restored);

    f.refresh_source_before_observation(&host_b);
    let fact_incarnation = f.observe(&token);
    let mut retired = f.launches[0].metadata.lock().committed_topology().clone();
    retired.nodes.get_mut(&node(4)).unwrap().lifecycle = ClusterNodeLifecycle::Decommissioned;
    f.commit(retired);
    let mut replacement = f.launches[0].metadata.lock().committed_topology().clone();
    let next = replacement.nodes.get_mut(&node(4)).unwrap();
    next.lifecycle = ClusterNodeLifecycle::Active;
    next.incarnation += 1;
    next.native_store_birth_v1 = None;
    f.commit(replacement);
    f.reject_without_proposal(
        &fact_incarnation,
        "identity/pins/schema/restore/birth changed",
    );
    factory_a
        .database
        .read()
        .require_native_metadata_uninitialized()
        .unwrap();
    host_b.shutdown().unwrap();
    f.shutdown();
}

#[test]
fn native_registered_learner_enrollment_rejects_original_canceled_or_expired_fact_before_held_latches(
) {
    let f = EnrollmentFixture::new();
    let (_factory, host) = f.factory("actual-canceled-factory");
    let canceled = CancellationToken::uncancelable().child();
    let fact = f.observe(&canceled);
    let (current, business) = f.current();
    canceled.cancel();
    let _service = f.launches[0].service.lock();
    let _database = f.seeds[0].database.write();
    let mut store = f.launches[0].metadata.lock();
    let before = store.committed_topology().clone();
    let index = store.status().last_log_index;
    let started = std::time::Instant::now();
    let error = store
        .propose_observed_native_learner_enrollment(
            &fact,
            &current,
            business,
            &CancellationToken::uncancelable(),
        )
        .unwrap_err();
    assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(store.status().last_log_index, index);
    assert_eq!(store.committed_topology(), &before);
    drop(store);
    drop(_database);
    drop(_service);

    // Expiry is also the original real context's deadline, not a fabricated
    // authority or a caller-selected expiration field on a response DTO.
    let timed = CancellationToken::uncancelable()
        .child_with_deadline(std::time::Instant::now() + Duration::from_secs(2));
    let timed_fact = f.observe(&timed);
    let (still_current, still_business) = f.current();
    let remaining = timed
        .deadline()
        .unwrap()
        .saturating_duration_since(std::time::Instant::now());
    std::thread::sleep(remaining + Duration::from_millis(10));
    let _service = f.launches[0].service.lock();
    let _database = f.seeds[0].database.write();
    let mut store = f.launches[0].metadata.lock();
    let before = store.committed_topology().clone();
    let index = store.status().last_log_index;
    let started = std::time::Instant::now();
    let error = store
        .propose_observed_native_learner_enrollment(
            &timed_fact,
            &still_current,
            still_business,
            &CancellationToken::uncancelable(),
        )
        .unwrap_err();
    assert!(matches!(error, BicDbError::QueryTimedOut), "{error}");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(store.status().last_log_index, index);
    assert_eq!(store.committed_topology(), &before);
    drop(store);
    drop(_database);
    drop(_service);
    host.shutdown().unwrap();
    f.shutdown();
}

#[test]
fn native_registered_learner_enrollment_retries_only_exact_persisted_pending_entry() {
    let f = EnrollmentFixture::new();
    let (_factory, host) = f.factory("actual-pending-factory");
    let token = CancellationToken::uncancelable();
    let fact = f.observe(&token);
    let mut unrelated = f.launches[0].metadata.lock().committed_topology().clone();
    unrelated.generation += 1;
    unrelated.nodes.get_mut(&node(0)).unwrap().used_bytes += 1;
    f.launches[0]
        .metadata
        .lock()
        .propose_topology(unrelated)
        .unwrap();
    f.reject_without_proposal(&fact, "conflicts with another pending topology proposal");
    f.launches[0].heartbeat_initial_metadata(&token).unwrap();

    let fact = f.observe(&token);
    let (current, business) = f.current();
    let (pending, old_commit, before_c) = {
        let mut store = f.launches[0].metadata.lock();
        let before = store.status();
        let c = store.committed_topology().clone();
        let entry = store
            .propose_observed_native_learner_enrollment(&fact, &current, business, &token)
            .unwrap()
            .unwrap();
        assert!(entry.index > before.commit_index);
        assert_eq!(store.status().commit_index, before.commit_index);
        assert_eq!(store.committed_topology(), &c);
        (entry, before.commit_index, c)
    };
    // A separately fresh current MQ is mandatory even for an identical retry.
    let (renewed, renewed_business) = f.current();
    let retried = f.launches[0]
        .metadata
        .lock()
        .propose_observed_native_learner_enrollment(&fact, &renewed, renewed_business, &token)
        .unwrap()
        .unwrap();
    assert_eq!(retried, pending);
    assert_eq!(
        f.launches[0].metadata.lock().status().last_log_index,
        pending.index
    );
    assert_eq!(
        f.launches[0].metadata.lock().status().commit_index,
        old_commit
    );
    assert_eq!(
        f.launches[0].metadata.lock().committed_topology(),
        &before_c
    );
    let result = f.launches[0]
        .enroll_registered_native_learner(&node(4), &token)
        .unwrap();
    assert_eq!(result.birth(), fact.birth().unwrap());
    assert_eq!(result.committed_index(), pending.index);
    assert_eq!(
        f.launches[0].metadata.lock().status().last_log_index,
        pending.index,
        "finite retry commits the existing persisted entry, without another index"
    );
    host.shutdown().unwrap();
    f.shutdown();
}

mod metadata_transition_tests;

mod promotion_tests;
