//! Actual-core characterization, not an implementation or HA qualification.
//! Scripted topology publication, in-process replica transport, loopback/plaintext,
//! fsync=false. The explicit StrongGet healthy-value test is intentionally red
//! until an additive strong protocol is implemented; legacy Get stays eventual.
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
        let range = self.range(command.range_id, command.range_epoch)?;
        self.service(dst)?.lock().commit_range_write(&range, &range.leader, command)
    }
    fn certify_range_write(&self, dst: &ClusterNodeId, command: &RangeWriteCommand) -> Result<RangeWriteAck> {
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
            available: RwLock::new([node("n0"), node("n1"), node("n2")].into()) });
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
fn characterize_previously_recovered_minority_leader_get_is_not_fresh_authority() {
    let f = Fixture::new();
    let old = Arc::new(RwLock::new(f.initial.clone()));
    let old_coordinator = f.coordinator(0, Arc::clone(&old));
    assert_eq!(old_coordinator.recover_local_leadership(1).unwrap(), 1);
    let new = f.new_authority();
    *f.bus.topology.write() = new.clone();
    *f.bus.available.write() = [node("n1"), node("n2")].into();
    let new_view = Arc::new(RwLock::new(new));
    let next = f.coordinator(1, new_view);
    assert_eq!(next.recover_local_leadership(1).unwrap(), 1);
    let expected = json!({"acknowledged": "on-new-quorum"});
    f.commit(1, "minority", &expected);
    assert_eq!(f.db[2].read().get(CLUSTER_EVENTS_COLLECTION, "fixture/minority").unwrap().unwrap().metadata, expected);
    let listener = f.listener(0, Arc::clone(&old));
    assert_eq!(f.legacy_get(0, old, "minority"), None);
    listener.join().unwrap();
    println!("{{\"case\":\"old_recovered_minority\",\"legacy_absent\":true,\"new_quorum_commit_completed\":true,\"fresh_strong_authority_proven\":false}}");
}

#[test]
fn characterize_new_leader_before_catchup_then_healthy_old_command_epoch() {
    let f = Fixture::new();
    *f.bus.available.write() = [node("n0"), node("n2")].into();
    let old = f.coordinator(0, Arc::new(RwLock::new(f.initial.clone())));
    assert_eq!(old.recover_local_leadership(1).unwrap(), 1);
    let expected = json!({"acknowledged": "before-new-leader"});
    f.commit(0, "catchup", &expected);
    let new = f.new_authority();
    *f.bus.topology.write() = new.clone();
    *f.bus.available.write() = [node("n0"), node("n1"), node("n2")].into();
    let view = Arc::new(RwLock::new(new));
    let next = f.coordinator(1, Arc::clone(&view));
    let listener = f.listener(1, Arc::clone(&view));
    assert_eq!(f.legacy_get(1, Arc::clone(&view), "catchup"), None);
    assert_eq!(next.recover_local_leadership(1).unwrap(), 1);
    assert_eq!(f.legacy_get(1, Arc::clone(&view), "catchup"), Some(expected));
    let range = view.read().ranges.values().next().unwrap().clone();
    let progress = f.services[1].lock().range_write_progress(range.id);
    assert!(progress.current_epoch < range.epoch, "old commands need not be rewritten into routing epoch");
    assert_eq!(progress.resolved_through, 1);
    listener.join().unwrap();
    println!("{{\"case\":\"new_leader_catchup\",\"legacy_absent_before\":true,\"exact_value_after\":true,\"authority_epoch\":{},\"retained_command_epoch\":{},\"resolved_through\":{}}}", range.epoch, progress.current_epoch, progress.resolved_through);
}

fn raw_strong_get(address: std::net::SocketAddr, cluster: &ClusterId, epoch: u64) -> std::result::Result<Value, String> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(1)).map_err(|e| e.to_string())?;
    stream.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(1))).unwrap();
    let body = serde_json::to_vec(&json!({"protocol_version": CLUSTER_DATA_PROTOCOL_VERSION,
        "cluster_id": cluster, "caller_node_id":"n0", "destination_node_id":"n0", "request_id":100,
        "request":{"ApplicationEvent":{"range_id":1,"range_epoch":epoch,
            "operation":{"operation":"strong_get","namespace":"fixture","key":"healthy"}}}})).unwrap();
    stream.write_all(&(body.len() as u32).to_be_bytes()).map_err(|e| e.to_string())?;
    stream.write_all(&body).map_err(|e| e.to_string())?;
    let mut size = [0;4]; stream.read_exact(&mut size).map_err(|e| e.to_string())?;
    let count = u32::from_be_bytes(size) as usize;
    if count == 0 || count > 1024 * 1024 { return Err("invalid response size".into()); }
    let mut response = vec![0;count]; stream.read_exact(&mut response).map_err(|e| e.to_string())?;
    serde_json::from_slice(&response).map_err(|e| e.to_string())
}

#[test]
fn target_explicit_strong_get_must_serve_a_healthy_quorum_value() {
    let f = Fixture::new();
    let view = Arc::new(RwLock::new(f.initial.clone()));
    let coordinator = f.coordinator(0, Arc::clone(&view));
    assert_eq!(coordinator.recover_local_leadership(1).unwrap(), 1);
    let expected = json!({"healthy":true});
    f.commit(0, "healthy", &expected);
    let listener = f.listener(0, Arc::clone(&view));
    assert_eq!(f.legacy_get(0, Arc::clone(&view), "healthy"), Some(expected.clone()));
    let epoch = view.read().ranges.values().next().unwrap().epoch;
    let response = raw_strong_get(listener.local_addr(), &f.config.cluster_id, epoch);
    listener.join().unwrap();
    assert_eq!(response.as_ref().ok().and_then(|r| r.pointer("/response/Ok/ApplicationEvent/value")),
        Some(&expected), "P0 strong contract is not implemented; an unavailable/unsupported healthy read cannot pass: {response:?}");
}
