//! Actual frozen-core RF3 certificate cut, not a physical HA test.
//! In-process replica transport, scripted authority publication, fsync=false.
//! The target recovery invariant is deliberately red on unmodified463.
use bicdb_core::*;
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};
use std::{collections::{BTreeMap, BTreeSet}, io::{Read, Write}, net::TcpStream,
          sync::Arc, time::Duration};

fn node(name: &str) -> ClusterNodeId { ClusterNodeId::new(name).unwrap() }
fn unavailable() -> BicDbError { BicDbError::Cluster("scripted unavailable voter".into()) }

#[derive(Debug)]
struct Bus {
    topology: Arc<RwLock<ClusterTopology>>,
    nodes: BTreeMap<ClusterNodeId, Arc<Mutex<ClusterDataNodeService>>>,
    available: RwLock<BTreeSet<ClusterNodeId>>,
    blocked_commit: RwLock<BTreeSet<ClusterNodeId>>,
    blocked_certify: RwLock<BTreeSet<ClusterNodeId>>,
    phases: Mutex<Vec<Value>>,
}
impl Bus {
    fn range(&self, id: RangeId, epoch: u64) -> Result<RangeDescriptor> {
        self.topology.read().range_by_id(id).filter(|r| r.epoch == epoch)
            .cloned().ok_or_else(unavailable)
    }
    fn service(&self, id: &ClusterNodeId) -> Result<Arc<Mutex<ClusterDataNodeService>>> {
        if !self.available.read().contains(id) { return Err(unavailable()); }
        self.nodes.get(id).cloned().ok_or_else(unavailable)
    }
}
impl RangeWriteTransport for Bus {
    fn range_write_progress(&self, dst: &ClusterNodeId, id: RangeId, epoch: u64) -> Result<RangeWriteProgress> {
        let range = self.range(id, epoch)?;
        self.service(dst)?.lock().range_write_progress_for_peer(&range, &range.leader)
    }
    fn apply_range_write_repair(&self, dst: &ClusterNodeId, batch: &RangeWriteRepairBatch, limits: &RangeWriteRepairLimits) -> Result<RangeWriteProgress> {
        let range = self.range(batch.range_id, batch.range_epoch)?;
        self.service(dst)?.lock().apply_range_write_repair(&range, &range.leader, batch, limits)
    }
    fn fetch_range_write_repair(&self, src: &ClusterNodeId, id: RangeId, epoch: u64, previous: u64, limits: &RangeWriteRepairLimits) -> Result<RangeWriteRepairBatch> {
        let range = self.range(id, epoch)?;
        self.service(src)?.lock().export_range_write_repair(&range, &range.leader, previous, limits)
    }
    fn probe_range_write(&self, src: &ClusterNodeId, id: RangeId, epoch: u64, index: u64) -> Result<RangeWriteProbe> {
        let range = self.range(id, epoch)?;
        self.service(src)?.lock().probe_range_write_for_peer(&range, &range.leader, index)
    }
    fn prepare_range_write(&self, dst: &ClusterNodeId, command: &RangeWriteCommand) -> Result<RangeWriteAck> {
        let range = self.range(command.range_id, command.range_epoch)?;
        self.service(dst)?.lock().prepare_range_write(&range, &range.leader, command.clone())
    }
    fn commit_range_write(&self, dst: &ClusterNodeId, command: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.phases.lock().push(json!({"phase":"commit", "destination":dst,"injected_failure":self.blocked_commit.read().contains(dst)}));
        if self.blocked_commit.read().contains(dst) { return Err(unavailable()); }
        let range = self.range(command.range_id, command.range_epoch)?;
        self.service(dst)?.lock().commit_range_write(&range, &range.leader, command)
    }
    fn certify_range_write(&self, dst: &ClusterNodeId, command: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.phases.lock().push(json!({"phase":"certify", "destination":dst,"injected_failure":self.blocked_certify.read().contains(dst)}));
        if self.blocked_certify.read().contains(dst) { return Err(unavailable()); }
        let range = self.range(command.range_id, command.range_epoch)?;
        self.service(dst)?.lock().certify_range_write(&range, &range.leader, command)
    }
    fn apply_range_write(&self, dst: &ClusterNodeId, command: &RangeWriteCommand) -> Result<RangeWriteAck> {
        let range = self.range(command.range_id, command.range_epoch)?;
        self.service(dst)?.lock().apply_certified_range_write(&range, &range.leader, command)
    }
    fn abort_range_write(&self, dst: &ClusterNodeId, command: &RangeWriteCommand) -> Result<RangeWriteAck> {
        let range = self.range(command.range_id, command.range_epoch)?;
        self.service(dst)?.lock().abort_range_write(&range, &range.leader, command)
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    config: DistributionConfig,
    db: Vec<Arc<RwLock<BicDb>>>,
    services: Vec<Arc<Mutex<ClusterDataNodeService>>>,
    initial: ClusterTopology,
    bus: Arc<Bus>,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let config = DistributionConfig {
            enabled: true, cluster_id: ClusterId::new("strong-characterization").unwrap(),
            node_id: node("n0"), initial_ranges: 1, replication_factor: 3,
            transport: ClusterNetworkTransportConfig { dev_localhost_plaintext: true,
                io_timeout_ms: 500, connect_timeout_ms: 100, ..Default::default() },
            ..Default::default()
        };
        let store = DistributionStore::initialize_at(root.path().join("metadata"), config.clone(), false, 1).unwrap();
        let mut initial = store.topology().clone();
        for id in ["n1", "n2"] {
            initial.nodes.insert(node(id), ClusterNode::new(node(id), "127.0.0.1:1", 1, 1_000_000, 1).unwrap());
            for range in initial.ranges.values_mut() {
                range.replicas.push(RangeReplica { id: ReplicaId::new(initial.next_replica_id).unwrap(),
                    node_id: node(id), role: RangeReplicaRole::Voter });
                initial.next_replica_id += 1;
            }
        }
        initial.validate().unwrap();
        let mut db = Vec::new();
        let mut services = Vec::new();
        for index in 0..3 {
            let directory = root.path().join(format!("node-{index}"));
            let database = Arc::new(RwLock::new(BicDb::open_with_config(&directory,
                DbConfig::default().with_fsync(false).with_storage_mode(StorageMode::ServerPaged)
                    .with_paged_buffer_pool_bytes(8 * 1024 * 1024).with_required_commit_admission(true)).unwrap()));
            database.write().create_collection(CLUSTER_EVENTS_COLLECTION).unwrap();
            let service = Arc::new(Mutex::new(ClusterDataNodeService::open(config.cluster_id.clone(),
                node(&format!("n{index}")), directory, Arc::clone(&database), false).unwrap()));
            db.push(database); services.push(service);
        }
        let bus = Arc::new(Bus { topology: Arc::new(RwLock::new(initial.clone())),
            nodes: (0..3).map(|i| (node(&format!("n{i}")), Arc::clone(&services[i]))).collect(),
            available: RwLock::new([node("n0"), node("n1"), node("n2")].into()),
            blocked_commit: RwLock::new(BTreeSet::new()), blocked_certify: RwLock::new(BTreeSet::new()), phases: Mutex::new(Vec::new()) });
        Self { _root: root, config, db, services, initial, bus }
    }
    fn coordinator(&self, index: usize, topology: Arc<RwLock<ClusterTopology>>) -> Arc<RangeWriteCoordinator<Bus>> {
        let coordinator = Arc::new(RangeWriteCoordinator::new(self.config.cluster_id.clone(),
            node(&format!("n{index}")), topology, Arc::clone(&self.services[index]), Arc::clone(&self.bus)).unwrap());
        self.db[index].read().install_commit_admission(coordinator.clone());
        coordinator
    }
    fn new_authority(&self) -> ClusterTopology {
        let mut topology = self.initial.clone();
        topology.generation += 1;
        let range = topology.ranges.values_mut().next().unwrap();
        range.epoch += 1; range.leader = node("n1");
        topology.validate().unwrap(); topology
    }
    fn commit(&self, index: usize, key: &str, value: &Value) {
        let database = self.db[index].read();
        let mut transaction = database.begin_transaction().unwrap();
        transaction.insert(CLUSTER_EVENTS_COLLECTION,
            Record::new(format!("fixture/{key}")).with_metadata(value.clone())).unwrap();
        transaction.commit().unwrap();
    }
    fn listener(&self, index: usize, topology: Arc<RwLock<ClusterTopology>>) -> ClusterDataServerHandle {
        let listener = start_cluster_data_server("127.0.0.1:0", self.config.transport.clone(),
            Arc::clone(&topology), Arc::clone(&self.services[index]), None, None, None).unwrap();
        topology.write().nodes.get_mut(&node(&format!("n{index}"))).unwrap().address = listener.local_addr().to_string();
        listener
    }
    fn legacy_get(&self, index: usize, topology: Arc<RwLock<ClusterTopology>>, key: &str) -> Option<Value> {
        let range = topology.read().ranges.values().next().unwrap().clone();
        let transport = TcpClusterRelocationTransport::new(self.config.cluster_id.clone(), node("n0"),
            topology, self.config.transport.clone()).unwrap();
        match transport.application_event(&node(&format!("n{index}")), ClusterEventRequest {
            range_id: range.id, range_epoch: range.epoch,
            operation: ClusterEventOperation::Get { namespace: "fixture".into(), key: key.into() } }).unwrap() {
            ClusterEventResponse::Value { value } => value,
            other => panic!("unexpected legacy response {other:?}"),
        }
    }
}

#[test]
fn target_acknowledged_rf3_write_recovers_with_only_leader_unavailable() {
    let f = Fixture::new();
    let old_view = Arc::new(RwLock::new(f.initial.clone()));
    let old = f.coordinator(0, old_view);
    old.recover_local_leadership(1).unwrap();
    f.bus.blocked_commit.write().insert(node("n2"));
    f.bus.blocked_certify.write().insert(node("n1"));
    let expected = json!({"acknowledged": "certificate-cut"});
    f.commit(0, "certificate-cut", &expected);
    let range = f.initial.ranges.values().next().unwrap().clone();
    let probes: Vec<_> = (0..3).map(|i| f.services[i].lock()
        .probe_range_write_for_peer(&range, &range.leader, 1).unwrap()).collect();
    assert_eq!(probes[0].entry.as_ref().unwrap().state, RangeWriteState::Applied);
    assert_eq!(probes[1].entry.as_ref().unwrap().state, RangeWriteState::Committed);
    assert_eq!(probes[2].entry.as_ref().unwrap().state, RangeWriteState::Prepared);
    assert_eq!(f.db[0].read().get(CLUSTER_EVENTS_COLLECTION,"fixture/certificate-cut").unwrap().unwrap().metadata, expected);
    let new = f.new_authority();
    *f.bus.topology.write() = new.clone();
    *f.bus.available.write() = [node("n1"),node("n2")].into();
    f.bus.blocked_commit.write().clear();
    f.bus.blocked_certify.write().clear();
    let next = f.coordinator(1, Arc::new(RwLock::new(new)));
    let recoveries: Vec<_> = (0..3).map(|_| next.recover_local_leadership(1)).collect();
    let recovery = &recoveries[0];
    println!("{}",json!({"client_commit_returned_success":true,"pre_loss_probes":probes,"only_unavailable_node":"n0","surviving_voters":2,"recovery":format!("{recovery:?}"),"three_recovery_attempts":recoveries.iter().map(|r|format!("{r:?}")).collect::<Vec<_>>(),"replication_phase_calls":f.bus.phases.lock().clone(),"expected_payload":expected,"authority_after_loss":f.bus.topology.read().clone(),"physical_ha_qualification":false}));
    assert!(recovery.is_ok(), "P0 RF3 ACK recoverability invariant: a healthy surviving quorum must recover every prior ACK after only leader loss: {recovery:?}");
    assert_eq!(f.db[1].read().get(CLUSTER_EVENTS_COLLECTION,"fixture/certificate-cut").unwrap().unwrap().metadata,expected);
}
