//! Actual-core certificate ACK/recovery regressions. Local fsync-on paged
//! stores and in-process scripted phase faults; not physical HA qualification.
use bicdb_core::*;
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

fn node(name: &str) -> ClusterNodeId {
    ClusterNodeId::new(name).unwrap()
}
fn unavailable() -> BicDbError {
    BicDbError::Cluster("scripted unavailable voter".into())
}

#[derive(Debug)]
struct Bus {
    topology: Arc<RwLock<ClusterTopology>>,
    nodes: BTreeMap<ClusterNodeId, Arc<Mutex<ClusterDataNodeService>>>,
    available: RwLock<BTreeSet<ClusterNodeId>>,
    blocked_commit: RwLock<BTreeSet<ClusterNodeId>>,
    blocked_certify: RwLock<BTreeSet<ClusterNodeId>>,
    lost_certify_reply: RwLock<BTreeSet<ClusterNodeId>>,
    phases: Mutex<Vec<Value>>,
}
impl Bus {
    fn range(&self, id: RangeId, epoch: u64) -> Result<RangeDescriptor> {
        self.topology
            .read()
            .range_by_id(id)
            .filter(|r| r.epoch == epoch)
            .cloned()
            .ok_or_else(unavailable)
    }
    fn service(&self, id: &ClusterNodeId) -> Result<Arc<Mutex<ClusterDataNodeService>>> {
        if !self.available.read().contains(id) {
            return Err(unavailable());
        }
        self.nodes.get(id).cloned().ok_or_else(unavailable)
    }
}
impl RangeWriteTransport for Bus {
    fn range_write_progress(
        &self,
        dst: &ClusterNodeId,
        id: RangeId,
        epoch: u64,
    ) -> Result<RangeWriteProgress> {
        let range = self.range(id, epoch)?;
        self.service(dst)?
            .lock()
            .range_write_progress_for_peer(&range, &range.leader)
    }
    fn apply_range_write_repair(
        &self,
        dst: &ClusterNodeId,
        batch: &RangeWriteRepairBatch,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        let range = self.range(batch.range_id, batch.range_epoch)?;
        self.service(dst)?
            .lock()
            .apply_range_write_repair(&range, &range.leader, batch, limits)
    }
    fn fetch_range_write_repair(
        &self,
        src: &ClusterNodeId,
        id: RangeId,
        epoch: u64,
        previous: u64,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        let range = self.range(id, epoch)?;
        self.service(src)?
            .lock()
            .export_range_write_repair(&range, &range.leader, previous, limits)
    }
    fn probe_range_write(
        &self,
        src: &ClusterNodeId,
        id: RangeId,
        epoch: u64,
        index: u64,
    ) -> Result<RangeWriteProbe> {
        let range = self.range(id, epoch)?;
        self.service(src)?
            .lock()
            .probe_range_write_for_peer(&range, &range.leader, index)
    }
    fn prepare_range_write(
        &self,
        dst: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(command.range_id, command.range_epoch)?;
        self.service(dst)?
            .lock()
            .prepare_range_write(&range, &range.leader, command.clone())
    }
    fn commit_range_write(
        &self,
        dst: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.phases.lock().push(json!({"phase":"commit", "destination":dst,"injected_failure":self.blocked_commit.read().contains(dst)}));
        if self.blocked_commit.read().contains(dst) {
            return Err(unavailable());
        }
        let range = self.range(command.range_id, command.range_epoch)?;
        self.service(dst)?
            .lock()
            .commit_range_write(&range, &range.leader, command)
    }
    fn certify_range_write(
        &self,
        dst: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.phases.lock().push(json!({"phase":"certify", "destination":dst,"injected_failure":self.blocked_certify.read().contains(dst)}));
        if self.blocked_certify.read().contains(dst) {
            return Err(unavailable());
        }
        let range = self.range(command.range_id, command.range_epoch)?;
        let ack = self
            .service(dst)?
            .lock()
            .certify_range_write(&range, &range.leader, command)?;
        if self.lost_certify_reply.read().contains(dst) {
            return Err(unavailable());
        }
        Ok(ack)
    }
    fn apply_range_write(
        &self,
        dst: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(command.range_id, command.range_epoch)?;
        self.service(dst)?
            .lock()
            .apply_certified_range_write(&range, &range.leader, command)
    }
    fn abort_range_write(
        &self,
        dst: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(command.range_id, command.range_epoch)?;
        self.service(dst)?
            .lock()
            .abort_range_write(&range, &range.leader, command)
    }
}

struct Fixture {
    config: DistributionConfig,
    db: Vec<Arc<RwLock<BicDb>>>,
    services: Vec<Arc<Mutex<ClusterDataNodeService>>>,
    initial: ClusterTopology,
    bus: Arc<Bus>,
}
impl Fixture {
    fn new(root: &Path) -> Self {
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("strong-characterization").unwrap(),
            node_id: node("n0"),
            initial_ranges: 1,
            replication_factor: 3,
            transport: ClusterNetworkTransportConfig {
                dev_localhost_plaintext: true,
                io_timeout_ms: 10_000,
                connect_timeout_ms: 100,
                ..Default::default()
            },
            ..Default::default()
        };
        let store =
            DistributionStore::initialize_at(root.join("metadata"), config.clone(), true, 1)
                .unwrap();
        let mut initial = store.topology().clone();
        for id in ["n1", "n2"] {
            initial.nodes.insert(
                node(id),
                ClusterNode::new(node(id), "127.0.0.1:1", 1, 1_000_000, 1).unwrap(),
            );
            for range in initial.ranges.values_mut() {
                range.replicas.push(RangeReplica {
                    id: ReplicaId::new(initial.next_replica_id).unwrap(),
                    node_id: node(id),
                    role: RangeReplicaRole::Voter,
                });
                initial.next_replica_id += 1;
            }
        }
        initial.validate().unwrap();
        let mut db = Vec::new();
        let mut services = Vec::new();
        for index in 0..3 {
            let directory = root.join(format!("node-{index}"));
            let database = Arc::new(RwLock::new(
                BicDb::open_with_config(
                    &directory,
                    DbConfig::default()
                        .with_fsync(true)
                        .with_storage_mode(StorageMode::ServerPaged)
                        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
                        .with_required_commit_admission(true),
                )
                .unwrap(),
            ));
            match database
                .write()
                .create_collection(CLUSTER_EVENTS_COLLECTION)
            {
                Ok(()) | Err(BicDbError::CollectionAlreadyExists(_)) => {}
                other => other.unwrap(),
            }
            let service = Arc::new(Mutex::new(
                ClusterDataNodeService::open(
                    config.cluster_id.clone(),
                    node(&format!("n{index}")),
                    directory,
                    Arc::clone(&database),
                    true,
                )
                .unwrap(),
            ));
            db.push(database);
            services.push(service);
        }
        let bus = Arc::new(Bus {
            topology: Arc::new(RwLock::new(initial.clone())),
            nodes: (0..3)
                .map(|i| (node(&format!("n{i}")), Arc::clone(&services[i])))
                .collect(),
            available: RwLock::new([node("n0"), node("n1"), node("n2")].into()),
            blocked_commit: RwLock::new(BTreeSet::new()),
            blocked_certify: RwLock::new(BTreeSet::new()),
            lost_certify_reply: RwLock::new(BTreeSet::new()),
            phases: Mutex::new(Vec::new()),
        });
        Self {
            config,
            db,
            services,
            initial,
            bus,
        }
    }
    fn coordinator(
        &self,
        index: usize,
        topology: Arc<RwLock<ClusterTopology>>,
    ) -> Arc<RangeWriteCoordinator<Bus>> {
        let coordinator = Arc::new(
            RangeWriteCoordinator::new(
                self.config.cluster_id.clone(),
                node(&format!("n{index}")),
                topology,
                Arc::clone(&self.services[index]),
                Arc::clone(&self.bus),
            )
            .unwrap(),
        );
        self.db[index]
            .read()
            .install_commit_admission(coordinator.clone());
        coordinator
    }
    fn new_authority(&self) -> ClusterTopology {
        let mut topology = self.initial.clone();
        topology.generation += 1;
        let range = topology.ranges.values_mut().next().unwrap();
        range.epoch += 1;
        range.leader = node("n1");
        topology.validate().unwrap();
        topology
    }
    fn commit(
        &self,
        index: usize,
        key: &str,
        value: &Value,
    ) -> (Result<()>, TransactionOutcomeObserver) {
        let database = self.db[index].read();
        let mut transaction = database.begin_transaction().unwrap();
        transaction
            .insert(
                CLUSTER_EVENTS_COLLECTION,
                Record::new(format!("fixture/{key}")).with_metadata(value.clone()),
            )
            .unwrap();
        let observer = transaction.observe_outcome();
        (transaction.commit(), observer)
    }
    fn value(&self, index: usize, key: &str) -> Option<Value> {
        self.db[index]
            .read()
            .get(CLUSTER_EVENTS_COLLECTION, &format!("fixture/{key}"))
            .unwrap()
            .map(|r| r.metadata.clone())
    }
    fn clear_admissions(&self) {
        for database in &self.db {
            database.read().clear_commit_admission();
        }
    }
    fn clear_faults(&self) {
        self.bus.blocked_commit.write().clear();
        self.bus.blocked_certify.write().clear();
        self.bus.lost_certify_reply.write().clear();
    }
    fn append(
        &self,
        topology: Arc<RwLock<ClusterTopology>>,
        key: &str,
        value: &Value,
    ) -> Result<ClusterEventResponse> {
        let range = topology.read().ranges.values().next().unwrap().clone();
        let transport = TcpClusterRelocationTransport::new(
            self.config.cluster_id.clone(),
            node("n0"),
            topology,
            self.config.transport.clone(),
        )
        .unwrap();
        transport.application_event(
            &range.leader,
            ClusterEventRequest {
                range_id: range.id,
                range_epoch: range.epoch,
                operation: ClusterEventOperation::Append {
                    namespace: "fixture".into(),
                    key: key.into(),
                    value: value.clone(),
                },
            },
        )
    }
    fn listener(
        &self,
        index: usize,
        topology: Arc<RwLock<ClusterTopology>>,
    ) -> ClusterDataServerHandle {
        let listener = start_cluster_data_server(
            "127.0.0.1:0",
            self.config.transport.clone(),
            Arc::clone(&topology),
            Arc::clone(&self.services[index]),
            None,
            None,
            None,
        )
        .unwrap();
        topology
            .write()
            .nodes
            .get_mut(&node(&format!("n{index}")))
            .unwrap()
            .address = listener.local_addr().to_string();
        listener
    }
}

#[test]
fn certificate_gap_returns_unknown_after_local_wal_then_background_recovery_progresses() {
    let root = tempfile::tempdir().unwrap();
    let f = Fixture::new(root.path());
    let coordinator = f.coordinator(0, Arc::new(RwLock::new(f.initial.clone())));
    coordinator.recover_local_leadership(1).unwrap();
    f.bus.blocked_commit.write().insert(node("n2"));
    f.bus.blocked_certify.write().insert(node("n1"));
    let expected = json!({"certificate":"gap"});
    let (result, receipt) = f.commit(0, "gap", &expected);
    let (sequence, incarnation) = match result.unwrap_err() {
        BicDbError::CommitOutcomeUnknown {
            sequence: Some(sequence),
            incarnation,
            ..
        } => (sequence, incarnation),
        other => panic!("must preserve unknown outcome: {other:?}"),
    };
    assert_eq!(
        receipt.outcome(),
        TransactionOutcome::Committed { sequence }
    );
    assert_eq!(incarnation, f.db[0].read().incarnation_id());
    assert_eq!(f.value(0, "gap"), Some(expected.clone()));
    assert!(f
        .commit(0, "too-soon", &json!({}))
        .0
        .unwrap_err()
        .to_string()
        .contains("recovery is not complete"));
    f.clear_faults();
    assert_eq!(coordinator.recover_local_leadership(1).unwrap(), 1);
    assert_eq!(f.value(0, "gap"), Some(expected.clone()));
    f.commit(0, "after-recovery", &json!({"progress":true}))
        .0
        .unwrap();
    for index in 0..3 {
        assert_eq!(f.value(index, "gap"), Some(expected.clone()));
    }
    f.clear_admissions();
}

#[test]
fn acknowledged_certificate_quorum_survives_only_leader_loss() {
    let root = tempfile::tempdir().unwrap();
    let f = Fixture::new(root.path());
    let old = f.coordinator(0, Arc::new(RwLock::new(f.initial.clone())));
    old.recover_local_leadership(1).unwrap();
    f.bus.blocked_commit.write().insert(node("n2"));
    let expected = json!({"acknowledged":"quorum"});
    f.commit(0, "survivor", &expected).0.unwrap();
    let new = f.new_authority();
    *f.bus.topology.write() = new.clone();
    *f.bus.available.write() = [node("n1"), node("n2")].into();
    f.clear_faults();
    let next = f.coordinator(1, Arc::new(RwLock::new(new)));
    assert_eq!(next.recover_local_leadership(1).unwrap(), 1);
    assert_eq!(f.value(1, "survivor"), Some(expected.clone()));
    f.commit(1, "after-failover", &json!({"progress":true}))
        .0
        .unwrap();
    for index in 1..3 {
        assert_eq!(f.value(index, "survivor"), Some(expected.clone()));
    }
    f.clear_admissions();
}

#[test]
fn lost_certificate_reply_stays_unknown_and_surviving_certificate_recovers() {
    let root = tempfile::tempdir().unwrap();
    let f = Fixture::new(root.path());
    let old = f.coordinator(0, Arc::new(RwLock::new(f.initial.clone())));
    old.recover_local_leadership(1).unwrap();
    f.bus.blocked_commit.write().insert(node("n2"));
    f.bus.lost_certify_reply.write().insert(node("n1"));
    let expected = json!({"certificate":"reply-lost"});
    assert!(matches!(
        f.commit(0, "lost-reply", &expected).0,
        Err(BicDbError::CommitOutcomeUnknown { .. })
    ));
    let range = f.initial.ranges.values().next().unwrap();
    assert_eq!(
        f.services[1]
            .lock()
            .probe_range_write_for_peer(range, &range.leader, 1)
            .unwrap()
            .entry
            .unwrap()
            .state,
        RangeWriteState::QuorumCommitted
    );
    let new = f.new_authority();
    *f.bus.topology.write() = new.clone();
    *f.bus.available.write() = [node("n1"), node("n2")].into();
    f.clear_faults();
    let next = f.coordinator(1, Arc::new(RwLock::new(new)));
    next.recover_local_leadership(1).unwrap();
    for index in 1..3 {
        assert_eq!(f.value(index, "lost-reply"), Some(expected.clone()));
    }
    f.clear_admissions();
}

#[test]
fn matching_event_retry_cannot_bypass_unknown_certificate_outcome() {
    let root = tempfile::tempdir().unwrap();
    let f = Fixture::new(root.path());
    let view = Arc::new(RwLock::new(f.initial.clone()));
    let old = f.coordinator(0, Arc::clone(&view));
    old.recover_local_leadership(1).unwrap();
    f.bus.blocked_commit.write().insert(node("n2"));
    f.bus.blocked_certify.write().insert(node("n1"));
    let expected = json!({"retry":"same-key"});
    assert!(matches!(
        f.commit(0, "retry", &expected).0,
        Err(BicDbError::CommitOutcomeUnknown { .. })
    ));
    let listener = f.listener(0, Arc::clone(&view));
    let retry = f.append(Arc::clone(&view), "retry", &expected);
    assert!(
        retry.is_err(),
        "an existing local row alone must not manufacture an ACK: {retry:?}"
    );
    f.clear_faults();
    old.recover_local_leadership(1).unwrap();
    assert_eq!(
        f.append(Arc::clone(&view), "retry", &expected).unwrap(),
        ClusterEventResponse::Appended { inserted: false }
    );
    assert!(f.append(view, "retry", &json!({"wrong":"value"})).is_err());
    listener.join().unwrap();
    f.clear_admissions();
}

#[test]
fn reopened_matching_event_requires_recovered_admission_and_local_outcome_resolves() {
    let root = tempfile::tempdir().unwrap();
    let expected = json!({"retry":"reopen"});
    let (sequence, incarnation) = {
        let f = Fixture::new(root.path());
        let old = f.coordinator(0, Arc::new(RwLock::new(f.initial.clone())));
        old.recover_local_leadership(1).unwrap();
        f.bus.blocked_commit.write().insert(node("n2"));
        f.bus.blocked_certify.write().insert(node("n1"));
        let error = f.commit(0, "reopen", &expected).0.unwrap_err();
        f.clear_admissions();
        match error {
            BicDbError::CommitOutcomeUnknown {
                sequence: Some(sequence),
                incarnation,
                ..
            } => (sequence, incarnation),
            other => panic!("{other:?}"),
        }
    };
    let f = Fixture::new(root.path());
    let view = Arc::new(RwLock::new(f.initial.clone()));
    let next = f.coordinator(0, Arc::clone(&view));
    let listener = f.listener(0, Arc::clone(&view));
    assert_eq!(f.value(0, "reopen"), Some(expected.clone()));
    assert!(matches!(
        f.db[0]
            .read()
            .resolve_commit_outcome(&incarnation, sequence)
            .unwrap(),
        CommitResolution::Committed
    ));
    assert!(
        f.append(Arc::clone(&view), "reopen", &expected).is_err(),
        "durable local row and local resolver are not cluster ACK proof"
    );
    next.recover_local_leadership(1).unwrap();
    assert_eq!(
        f.append(view, "reopen", &expected).unwrap(),
        ClusterEventResponse::Appended { inserted: false }
    );
    listener.join().unwrap();
    f.clear_admissions();
}
