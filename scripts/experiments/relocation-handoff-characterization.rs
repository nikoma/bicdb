//! DRAFT: actual-core relocation handoff characterization; not yet executed.
//! No engine edits. Real commands/copy/catch-up/cleanup and five metadata stores.
//! In-process trusted delivery with fixed caller identities, scripted scheduling,
//! strict schema fences and fsync=true; no TLS/network/fault/production proof.
use bicdb_core::*;
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
fn node(name: &str) -> ClusterNodeId {
    ClusterNodeId::new(name).unwrap()
}
fn index(id: &ClusterNodeId) -> usize {
    id.as_str().strip_prefix('n').unwrap().parse().unwrap()
}
fn unavailable() -> BicDbError {
    BicDbError::Cluster("scripted unavailable destination".into())
}
fn stamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
fn digest(v: &Value) -> String {
    hex::encode(Sha256::digest(serde_json::to_vec(v).unwrap()))
}
#[derive(Debug)]
struct Network {
    services: Vec<Arc<Mutex<ClusterDataNodeService>>>,
    views: Vec<Arc<RwLock<ClusterTopology>>>,
    available: RwLock<BTreeSet<ClusterNodeId>>,
    metadata_leaders: RwLock<Vec<Option<ClusterNodeId>>>,
}
#[derive(Debug)]
struct RangeBus {
    net: Arc<Network>,
    caller: ClusterNodeId,
}
impl RangeBus {
    fn service(&self, id: &ClusterNodeId) -> Result<Arc<Mutex<ClusterDataNodeService>>> {
        if !self.net.available.read().contains(id) {
            return Err(unavailable());
        }
        Ok(self.net.services[index(id)].clone())
    }
    fn range(
        &self,
        destination: &ClusterNodeId,
        id: RangeId,
        epoch: u64,
    ) -> Result<RangeDescriptor> {
        self.net.views[index(destination)]
            .read()
            .range_by_id(id)
            .filter(|r| r.epoch == epoch)
            .cloned()
            .ok_or_else(unavailable)
    }
}
impl RangeWriteTransport for RangeBus {
    fn range_write_progress(
        &self,
        dst: &ClusterNodeId,
        id: RangeId,
        epoch: u64,
    ) -> Result<RangeWriteProgress> {
        let range = self.range(dst, id, epoch)?;
        self.service(dst)?
            .lock()
            .range_write_progress_for_peer(&range, &self.caller)
    }
    fn apply_range_write_repair(
        &self,
        dst: &ClusterNodeId,
        batch: &RangeWriteRepairBatch,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        let range = self.range(dst, batch.range_id, batch.range_epoch)?;
        self.service(dst)?
            .lock()
            .apply_range_write_repair(&range, &self.caller, batch, limits)
    }
    fn fetch_range_write_repair(
        &self,
        src: &ClusterNodeId,
        id: RangeId,
        epoch: u64,
        previous: u64,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        let range = self.range(src, id, epoch)?;
        self.service(src)?
            .lock()
            .export_range_write_repair(&range, &self.caller, previous, limits)
    }
    fn probe_range_write(
        &self,
        src: &ClusterNodeId,
        id: RangeId,
        epoch: u64,
        index: u64,
    ) -> Result<RangeWriteProbe> {
        let range = self.range(src, id, epoch)?;
        self.service(src)?
            .lock()
            .probe_range_write_for_peer(&range, &self.caller, index)
    }
    fn prepare_range_write(
        &self,
        dst: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(dst, command.range_id, command.range_epoch)?;
        self.service(dst)?
            .lock()
            .prepare_range_write(&range, &self.caller, command.clone())
    }
    fn commit_range_write(
        &self,
        dst: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(dst, command.range_id, command.range_epoch)?;
        self.service(dst)?
            .lock()
            .commit_range_write(&range, &self.caller, command)
    }
    fn certify_range_write(
        &self,
        dst: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(dst, command.range_id, command.range_epoch)?;
        self.service(dst)?
            .lock()
            .certify_range_write(&range, &self.caller, command)
    }
    fn apply_range_write(
        &self,
        dst: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(dst, command.range_id, command.range_epoch)?;
        self.service(dst)?
            .lock()
            .apply_certified_range_write(&range, &self.caller, command)
    }
    fn abort_range_write(
        &self,
        dst: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(dst, command.range_id, command.range_epoch)?;
        self.service(dst)?
            .lock()
            .abort_range_write(&range, &self.caller, command)
    }
}

impl Network {
    fn service(&self, id: &ClusterNodeId) -> Result<Arc<Mutex<ClusterDataNodeService>>> {
        if !self.available.read().contains(id) {
            return Err(unavailable());
        }
        Ok(self.services[index(id)].clone())
    }
    fn authority(
        &self,
        dst: &ClusterNodeId,
        actor: &ClusterNodeId,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
    ) -> Result<()> {
        let topology = self.views[index(dst)].read();
        if topology.relocations.get(&relocation.id) != Some(relocation)
            || topology.range_by_id(range.id) != Some(range)
        {
            return Err(BicDbError::Cluster(
                "scripted receiver rejects noncommitted relocation/descriptor".into(),
            ));
        }
        let source = actor == &range.leader || relocation.source.as_ref() == Some(actor);
        if !source {
            if self.metadata_leaders.read()[index(dst)].as_ref() != Some(actor) {
                return Err(BicDbError::Cluster(
                    "scripted receiver rejects nonleader controller".into(),
                ));
            }
            let n = topology.nodes.get(actor).ok_or_else(unavailable)?;
            if n.metadata_role != MetadataMemberRole::Voter
                || n.lifecycle != ClusterNodeLifecycle::Active
            {
                return Err(unavailable());
            }
        }
        Ok(())
    }
}
#[derive(Debug)]
struct RelocationBus {
    net: Arc<Network>,
    actor: ClusterNodeId,
}
impl ClusterRelocationTransport for RelocationBus {
    fn prepare_learner(
        &mut self,
        r: &RangeRelocation,
        g: &RangeDescriptor,
        c: &CancellationToken,
    ) -> Result<()> {
        c.check()?;
        self.net.authority(&g.leader, &self.actor, r, g)?;
        self.net.authority(&r.target, &self.actor, r, g)?;
        let source = self.net.service(&g.leader)?.lock().database();
        let bundle = source.read().cluster_schema_bundle()?;
        let target = self.net.service(&r.target)?;
        let mut target = target.lock();
        target.install_schema_bundle(r, g, &bundle)?;
        target.prepare_learner(r)
    }
    fn copy_snapshot_step(
        &mut self,
        r: &RangeRelocation,
        g: &RangeDescriptor,
        o: &RangeSnapshotOptions,
        c: &CancellationToken,
    ) -> Result<SnapshotCopyProgress> {
        c.check()?;
        self.net.authority(&g.leader, &self.actor, r, g)?;
        self.net.authority(&r.target, &self.actor, r, g)?;
        let step = self
            .net
            .service(&g.leader)?
            .lock()
            .export_snapshot_step(r, g, o)?;
        self.net
            .service(&r.target)?
            .lock()
            .apply_snapshot_step(r, g, &step)
    }
    fn catch_up_step(
        &mut self,
        r: &RangeRelocation,
        g: &RangeDescriptor,
        max: usize,
        c: &CancellationToken,
    ) -> Result<CatchUpProgress> {
        c.check()?;
        self.net.authority(&g.leader, &self.actor, r, g)?;
        self.net.authority(&r.target, &self.actor, r, g)?;
        let durable = self
            .net
            .service(&r.target)?
            .lock()
            .learner_durable_commit_sequence(r)?;
        let batch = self
            .net
            .service(&g.leader)?
            .lock()
            .export_catch_up(g, durable, max)?;
        self.net
            .service(&r.target)?
            .lock()
            .apply_catch_up(r, g, &batch)
    }
    fn cleanup_source(
        &mut self,
        r: &RangeRelocation,
        g: &RangeDescriptor,
        resume: Option<&str>,
        max: usize,
        c: &CancellationToken,
    ) -> Result<CleanupProgress> {
        c.check()?;
        self.net.authority(&r.target, &self.actor, r, g)?;
        let Some(source) = r.source.as_ref() else {
            self.net.service(&r.target)?.lock().complete_relocation(r)?;
            return Ok(CleanupProgress {
                resume_after_key: None,
                records_deleted: r.cleanup_records_deleted,
                completed: true,
            });
        };
        self.net.authority(source, &self.actor, r, g)?;
        let progress = self
            .net
            .service(source)?
            .lock()
            .cleanup_source_step(r, g, resume, max)?;
        if progress.completed {
            self.net.service(&r.target)?.lock().complete_relocation(r)?;
        }
        Ok(progress)
    }
}
fn driver(
    net: &Arc<Network>,
    actor: &ClusterNodeId,
) -> TransportClusterRelocationDriver<RelocationBus> {
    TransportClusterRelocationDriver::new(
        RelocationBus {
            net: net.clone(),
            actor: actor.clone(),
        },
        ClusterRelocationTransportConfig {
            snapshot: RangeSnapshotOptions {
                max_records_per_batch: 1,
                max_bytes_per_batch: 1024 * 1024,
                max_record_bytes: 1024 * 1024,
            },
            max_commit_frames_per_step: 128,
        },
    )
    .unwrap()
}
fn move_plan(
    store: &DistributionStore,
    source: Option<ClusterNodeId>,
    target: ClusterNodeId,
    time: u64,
) -> RebalancePlan {
    let r = store.topology().ranges.values().next().unwrap();
    RebalancePlan {
        id: format!("handoff-plan-{time}"),
        topology_generation: store.topology().generation,
        created_at_ms: time,
        replica_moves: vec![PlannedReplicaMove {
            range_id: r.id,
            expected_epoch: r.epoch,
            source: source.clone(),
            target,
            estimated_bytes: 0,
            reason: if source.is_none() {
                ReplicaMoveReason::UnderReplicated
            } else {
                ReplicaMoveReason::LoadBalance
            },
        }],
        leader_transfers: Vec::new(),
        unplaced_ranges: Vec::new(),
        estimated_bytes_in_flight: 0,
    }
}
struct Metadata {
    stores: Vec<MetadataConsensusStore>,
    leader: usize,
    network: Arc<Network>,
    exchanges: Vec<Value>,
}
impl Metadata {
    fn new(root: &Path, network: &Arc<Network>, initial: &ClusterTopology) -> Self {
        let mut stores = (0..5)
            .map(|i| {
                MetadataConsensusStore::open(
                    root.join(format!("consensus-{i}")),
                    initial.cluster_id.clone(),
                    node(&format!("n{i}")),
                    initial.clone(),
                    true,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let request = stores[2].start_election().unwrap();
        let mut votes = BTreeSet::from([node("n2")]);
        let mut exchanges = Vec::new();
        for i in [0, 1, 3, 4] {
            let response = stores[i].handle_vote_request(request.clone()).unwrap();
            assert!(response.vote_granted);
            votes.insert(response.voter_id.clone());
            exchanges.push(
                json!({"kind":"vote","unix_ns":stamp(),"request":request,"response":response}),
            );
        }
        stores[2].become_leader(&votes).unwrap();
        let mut s = Self {
            stores,
            leader: 2,
            network: network.clone(),
            exchanges,
        };
        s.replicate(&[0, 1, 2, 3, 4]);
        s
    }
    fn replicate(&mut self, recipients: &[usize]) {
        let leader = self.leader;
        for _ in 0..3 {
            for i in recipients.iter().copied().filter(|i| *i != leader) {
                let request = self.stores[self.leader]
                    .append_request_from(1, 128)
                    .unwrap();
                let response = self.stores[i]
                    .handle_append_request(request.clone())
                    .unwrap();
                assert!(response.success);
                let committed = self.stores[self.leader]
                    .record_append_response(&node(&format!("n{i}")), &response)
                    .unwrap();
                self.exchanges.push(json!({"kind":"append","unix_ns":stamp(),"destination":i,"request":request,"response":response,"leader_commit_advanced":committed}));
            }
        }
        for i in 0..5 {
            let status = self.stores[i].status();
            *self.network.views[i].write() = self.stores[i].committed_topology().clone();
            self.network.metadata_leaders.write()[i] = status.leader_id;
        }
    }
    fn commit(
        &mut self,
        authoritative: &mut DistributionStore,
        staged: DistributionStore,
        recipients: &[usize],
    ) {
        let proposal = self.stores[self.leader]
            .propose_topology(staged.topology().clone())
            .unwrap();
        self.exchanges
            .push(json!({"kind":"proposal","unix_ns":stamp(),"entry":proposal}));
        self.replicate(recipients);
        assert_eq!(
            self.stores[self.leader].committed_topology(),
            staged.topology()
        );
        authoritative
            .install_authoritative_topology(self.stores[self.leader].committed_topology().clone())
            .unwrap();
    }
}
struct Fixture {
    root: PathBuf,
    config: DistributionConfig,
    db: Vec<Arc<RwLock<BicDb>>>,
    net: Arc<Network>,
    store: DistributionStore,
    meta: Metadata,
    time: u64,
    events: Vec<Value>,
    started: Instant,
}
impl Fixture {
    fn push_event(&mut self, event: Value) {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("external-events.jsonl"))
            .unwrap();
        file.write_all(&serde_json::to_vec(&event).unwrap())
            .unwrap();
        file.write_all(b"\n").unwrap();
        file.sync_all().unwrap();
        self.events.push(event);
    }
    fn new(root: PathBuf) -> Self {
        assert!(!root.exists());
        std::fs::create_dir_all(&root).unwrap();
        let cluster = ClusterId::new("range-handoff-characterization").unwrap();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: cluster.clone(),
            node_id: node("n0"),
            initial_ranges: 1,
            replication_factor: 3,
            node_capacity_bytes: 1_000_000,
            ..Default::default()
        };
        let mut db = Vec::new();
        let mut services = Vec::new();
        let mut fingerprints = Vec::new();
        for i in 0..5 {
            let path = root.join(format!("node-{i}"));
            let cfg = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
                .with_required_commit_admission(true);
            let mut created = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            created
                .create_collection(CLUSTER_EVENTS_COLLECTION)
                .unwrap();
            created.close().unwrap();
            let database = Arc::new(RwLock::new(BicDb::open_with_config(&path, cfg).unwrap()));
            let service = Arc::new(Mutex::new(
                ClusterDataNodeService::open(
                    cluster.clone(),
                    node(&format!("n{i}")),
                    &path,
                    database.clone(),
                    true,
                )
                .unwrap(),
            ));
            fingerprints.push(
                service
                    .lock()
                    .schema_compatibility_fingerprint()
                    .unwrap()
                    .sha256,
            );
            db.push(database);
            services.push(service);
        }
        let mut store =
            DistributionStore::initialize_at(root.join("metadata"), config.clone(), true, 1)
                .unwrap();
        store
            .heartbeat(
                &node("n0"),
                1,
                0,
                1_000_000,
                BTreeMap::from([(
                    SCHEMA_COMPATIBILITY_NODE_LABEL.into(),
                    fingerprints[0].clone(),
                )]),
                2,
            )
            .unwrap();
        for i in 1..5 {
            store
                .join_node(
                    ClusterNode::new(
                        node(&format!("n{i}")),
                        format!("127.0.0.1:{}", 9444 + i),
                        1,
                        1_000_000,
                        3 + i as u64,
                    )
                    .unwrap()
                    .with_label(SCHEMA_COMPATIBILITY_NODE_LABEL, fingerprints[i].clone())
                    .unwrap(),
                    &node("n0"),
                    3 + i as u64,
                )
                .unwrap();
        }
        let initial = store.topology().clone();
        let net = Arc::new(Network {
            services,
            views: (0..5)
                .map(|_| Arc::new(RwLock::new(initial.clone())))
                .collect(),
            available: RwLock::new((0..5).map(|i| node(&format!("n{i}"))).collect()),
            metadata_leaders: RwLock::new(vec![None; 5]),
        });
        // Legitimate API-only bootstrap adds missing RF3 replicas using real empty
        // snapshots/catch-up. This precedes the common five-voter consensus genesis.
        let mut time = 20;
        for target in [1, 2] {
            let plan = move_plan(&store, None, node(&format!("n{target}")), time);
            let id = store
                .apply_rebalance_plan(&plan, &node("n0"), time)
                .unwrap()[0];
            let mut d = driver(&net, &node("n0"));
            for _ in 0..128 {
                for v in &net.views {
                    *v.write() = store.topology().clone();
                }
                if store.relocation(id).unwrap().phase == RelocationPhase::Completed {
                    break;
                }
                time += 1;
                d.drive_relocation(
                    &mut store,
                    id,
                    &node("n0"),
                    time,
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            }
            assert_eq!(
                store.relocation(id).unwrap().phase,
                RelocationPhase::Completed
            );
            time += 1;
        }
        assert_eq!(
            store
                .topology()
                .ranges
                .values()
                .next()
                .unwrap()
                .voter_count(),
            3
        );
        assert_eq!(store.topology().metadata_voters().len(), 5);
        let meta = Metadata::new(&root, &net, store.topology());
        Self {
            root,
            config,
            db,
            net,
            store,
            meta,
            time,
            events: Vec::new(),
            started: Instant::now(),
        }
    }
    fn coordinator(&self, i: usize) -> Arc<RangeWriteCoordinator<RangeBus>> {
        let c = Arc::new(
            RangeWriteCoordinator::new_schema_fenced(
                self.config.cluster_id.clone(),
                node(&format!("n{i}")),
                self.net.views[i].clone(),
                self.net.services[i].clone(),
                Arc::new(RangeBus {
                    net: self.net.clone(),
                    caller: node(&format!("n{i}")),
                }),
            )
            .unwrap(),
        );
        self.db[i].read().install_commit_admission(c.clone());
        c
    }
    fn commit_row(&mut self, i: usize, key: &str, value: &Value) -> u64 {
        let range = self.net.views[i].read().ranges.values().next().unwrap().id;
        let expected_index = self.net.services[i].lock().next_range_write_index(range);
        let before = stamp();
        {
            let database = self.db[i].read();
            let mut t = database.begin_transaction().unwrap();
            t.insert(
                CLUSTER_EVENTS_COLLECTION,
                Record::new(format!("fixture/{key}")).with_metadata(value.clone()),
            )
            .unwrap();
            t.commit().unwrap();
        }
        self.push_event(json!({"cut":"acknowledged_commit","key":key,"payload":value,"payload_sha256":digest(value),"range_index_expected_before_actual_commit":expected_index,"invoked_unix_ns":before,"acknowledged_unix_ns":stamp(),"source_db_commit_seq":self.db[i].read().current_commit_seq()}));
        expected_index
    }
    fn observe(&mut self, cut: &str, late_index: Option<u64>) {
        let mut nodes = Vec::new();
        for i in 0..5 {
            let topo = self.meta.stores[i].committed_topology();
            let range = topo.ranges.values().next().unwrap();
            let root = self.root.join(format!("node-{i}"));
            let path = root.join("cluster-range-write-log.json");
            let catalog = if path.exists() {
                RangeWriteStore::inspect(
                    &root,
                    self.config.cluster_id.clone(),
                    node(&format!("n{i}")),
                    8 * 1024 * 1024,
                )
                .unwrap();
                Some(serde_json::from_slice::<Value>(&std::fs::read(path).unwrap()).unwrap())
            } else {
                None
            };
            let lookup = |key: &str| {
                self.db[i]
                    .read()
                    .get(CLUSTER_EVENTS_COLLECTION, &format!("fixture/{key}"))
                    .unwrap()
                    .map(|r| r.metadata.clone())
            };
            let probe = late_index.map(|idx| {
                self.net.services[i]
                    .lock()
                    .probe_range_write_for_peer(range, &range.leader, idx)
                    .map(|p| serde_json::to_value(p).unwrap())
                    .unwrap_or_else(|e| json!({"unavailable":e.to_string()}))
            });
            nodes.push(json!({"node":i,"metadata":self.meta.stores[i].status(),"committed_range":range,"available_for_data_rpc":self.net.available.read().contains(&node(&format!("n{i}"))),"range_progress":self.net.services[i].lock().range_write_progress(range.id),"late_probe":probe,"catalog":catalog,"seed":lookup("seed"),"late":lookup("late")}));
        }
        self.push_event(json!({"cut":cut,"unix_ns":stamp(),"elapsed_ms":self.started.elapsed().as_secs_f64()*1000.0,"nodes":nodes,"authoritative_relocations":self.store.topology().relocations}));
    }
    fn change(
        &mut self,
        recipients: &[usize],
        operation: impl FnOnce(&mut DistributionStore, &ClusterNodeId, u64),
    ) {
        self.time += 1;
        let mut staged = self.store.fork_ephemeral();
        operation(&mut staged, &node("n2"), self.time);
        self.meta.commit(&mut self.store, staged, recipients);
    }
    fn drive(
        &mut self,
        d: &mut TransportClusterRelocationDriver<RelocationBus>,
        id: RelocationId,
        recipients: &[usize],
    ) {
        assert!(self.started.elapsed() < std::time::Duration::from_secs(300));
        self.time += 1;
        let mut staged = self.store.fork_ephemeral();
        d.drive_relocation(
            &mut staged,
            id,
            &node("n2"),
            self.time,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
        self.meta.commit(&mut self.store, staged, recipients);
    }
    fn checkpoint_and_clear_admission(&self) {
        for d in &self.db {
            d.read().clear_commit_admission();
        }
        for d in &self.db {
            d.read().checkpoint_for_resume().unwrap();
        }
    }
}
fn run_case(root: PathBuf, recover_before_cleanup: bool) -> Value {
    let mut f = Fixture::new(root);
    f.observe("common_genesis_before_user_writes", None);
    let old = f.coordinator(0);
    old.recover_local_leadership(1).unwrap();
    let seed = json!({"kind":"seed","value":"copied-before-cut"});
    let seed_index = f.commit_row(0, "seed", &seed);
    let mut relocation_id = None;
    f.change(&[0, 1, 2, 3, 4], |s, a, t| {
        let p = move_plan(s, Some(node("n0")), node("n3"), t);
        relocation_id = Some(s.apply_rebalance_plan(&p, a, t).unwrap()[0]);
    });
    let id = relocation_id.unwrap();
    old.recover_local_leadership(1).unwrap();
    let mut d = driver(&f.net, &node("n2"));
    for _ in 0..128 {
        if f.store.relocation(id).unwrap().phase == RelocationPhase::ReadyToPromote {
            break;
        }
        f.drive(&mut d, id, &[0, 1, 2, 3, 4]);
    }
    assert_eq!(
        f.store.relocation(id).unwrap().phase,
        RelocationPhase::ReadyToPromote
    );
    f.observe("real_final_catchup_ready_before_late_ack", None);
    let ready_source_sequence = f
        .store
        .relocation(id)
        .unwrap()
        .source_commit_sequence
        .unwrap();
    *f.net.available.write() = BTreeSet::from([node("n0"), node("n1"), node("n3"), node("n4")]);
    let late = json!({"kind":"late-ack","value":"after-final-copy-before-promotion"});
    let late_index = f.commit_row(0, "late", &late);
    let source_sequence_after_late_ack = f.db[0].read().current_commit_seq();
    assert!(source_sequence_after_late_ack > ready_source_sequence);
    assert!(late_index > seed_index);
    f.observe("late_ack_old_quorum_after_final_copy", Some(late_index));
    f.drive(&mut d, id, &[2, 3, 4]);
    assert_eq!(
        f.store.relocation(id).unwrap().phase,
        RelocationPhase::Promoted
    );
    let range = f.store.topology().ranges.values().next().unwrap();
    assert_eq!(range.voter_count(), 4);
    assert_eq!(range.leader, node("n3"));
    *f.net.available.write() = BTreeSet::from([node("n2"), node("n3"), node("n4")]);
    let next = f.coordinator(3);
    let transient = next.recover_local_leadership(1);
    assert!(
        transient.is_err(),
        "four-voter promotion guard must require three responses"
    );
    f.push_event(json!({"cut":"promoted_four_voter_quorum_guard","unix_ns":stamp(),"recovery":format!("{transient:?}")}));
    f.observe("promoted_four_voters_only_two_available", Some(late_index));
    // Deliver actual committed promotion to source0 so its cleanup request is
    // authorized by its own metadata state. Node1 remains stale and unavailable.
    f.meta.replicate(&[0, 2, 3, 4]);
    *f.net.available.write() = BTreeSet::from([node("n0"), node("n2"), node("n3"), node("n4")]);
    if recover_before_cleanup {
        let recovery = next.recover_local_leadership(1);
        assert!(recovery.is_ok());
        assert_eq!(
            f.db[3]
                .read()
                .get(CLUSTER_EVENTS_COLLECTION, "fixture/late")
                .unwrap()
                .unwrap()
                .metadata,
            late
        );
        f.push_event(json!({"cut":"healthy_control_recovers_with_source_before_cleanup","unix_ns":stamp(),"recovery":format!("{recovery:?}")}));
        f.observe(
            "healthy_control_late_ack_visible_before_cleanup",
            Some(late_index),
        );
    }
    for _ in 0..128 {
        if f.store.relocation(id).unwrap().phase == RelocationPhase::Completed {
            break;
        }
        f.drive(&mut d, id, &[2, 3, 4]);
        // The metadata majority commits first; source0 learns the already
        // committed result afterward so its actual cleanup RPC is authorized.
        f.meta.replicate(&[0]);
    }
    assert_eq!(
        f.store.relocation(id).unwrap().phase,
        RelocationPhase::Completed
    );
    assert_eq!(
        f.store
            .topology()
            .ranges
            .values()
            .next()
            .unwrap()
            .voter_count(),
        3
    );
    assert!(
        f.db[0]
            .read()
            .get(CLUSTER_EVENTS_COLLECTION, "fixture/late")
            .unwrap()
            .is_none(),
        "actual source cleanup must remove the row"
    );
    assert_eq!(
        f.db[1]
            .read()
            .get(CLUSTER_EVENTS_COLLECTION, "fixture/late")
            .unwrap()
            .unwrap()
            .metadata,
        late,
        "unavailable old copy is retained"
    );
    f.observe(
        "actual_cleanup_removed_source_from_new_configuration",
        Some(late_index),
    );
    let recovery = next.recover_local_leadership(1);
    let ready = recovery.is_ok();
    let visible = f.db[3]
        .read()
        .get(CLUSTER_EVENTS_COLLECTION, "fixture/late")
        .unwrap()
        .map(|r| r.metadata.clone());
    f.push_event(json!({"cut":"new_configuration_fresh_recovery","unix_ns":stamp(),"recovery":format!("{recovery:?}"),"late":visible}));
    f.observe("final_new_authority_observation", Some(late_index));
    if recover_before_cleanup {
        assert!(ready);
        assert_eq!(visible, Some(late.clone()));
    }
    let report = json!({"recover_before_cleanup_control":recover_before_cleanup,"seed_range_index":seed_index,"late_range_index":late_index,"copied_source_commit_sequence_at_ready":ready_source_sequence,"source_db_commit_seq_after_late_ack":source_sequence_after_late_ack,"late_payload":late,"late_payload_sha256":digest(&late),"new_leader_recovery_succeeded":ready,"new_leader_local_value":visible,"ready_with_absent_acknowledged_value":ready&&visible.is_none(),"old_node1_copy_retained":true,"irretrievable_data_loss_claimed":false,"azure_cause_proven":false,"physical_or_production_qualification":false,"events":f.events,"metadata_exchanges":f.meta.exchanges});
    std::fs::write(
        f.root.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    f.checkpoint_and_clear_admission();
    drop(old);
    drop(next);
    report
}
fn main() {
    let output = PathBuf::from(
        std::env::args()
            .nth(1)
            .expect("new task-owned output directory required"),
    );
    assert!(!output.exists());
    std::fs::create_dir_all(&output).unwrap();
    let control = run_case(output.join("control"), true);
    let cleanup = run_case(output.join("cleanup-first"), false);
    let summary = json!({"runtime_source":"d49a391b3c6157f90f29e9f2f5214143c60f9f4b","runtime_version":"1.0.463-beta","in_process_trusted_delivery":true,"tls_exercised":false,"fsync":true,"actual_metadata_stores":5,"actual_copy_catchup_cleanup":true,"forged_progress_or_catalogs":false,"control_ack_visible":control["new_leader_local_value"].is_object(),"cleanup_first_ready_with_absent_ack":cleanup["ready_with_absent_acknowledged_value"],"irretrievable_data_loss_claimed":false,"azure_cause_proven":false,"physical_or_production_qualification":false});
    std::fs::write(
        output.join("summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    println!("{summary}");
    if std::env::args().any(|a| a == "--assert-preserved") {
        assert_eq!(
            cleanup["new_leader_local_value"], control["new_leader_local_value"],
            "target guarantee: acknowledged value survives legitimate handoff"
        );
    }
}
