//! Actual frozen463 supported relocation/image-history characterization.
//! Real core commands/services, fsync=false; scripted initial metadata fixture.
use bicdb_core::*;
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
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
        let range = self.range(command.range_id, command.range_epoch)?;
        self.service(dst)?
            .lock()
            .certify_range_write(&range, &range.leader, command)
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
            enabled: true,
            cluster_id: ClusterId::new("strong-characterization").unwrap(),
            node_id: node("n0"),
            initial_ranges: 1,
            replication_factor: 3,
            transport: ClusterNetworkTransportConfig {
                dev_localhost_plaintext: true,
                io_timeout_ms: 500,
                connect_timeout_ms: 100,
                ..Default::default()
            },
            ..Default::default()
        };
        let store = DistributionStore::initialize_at(
            root.path().join("metadata"),
            config.clone(),
            false,
            1,
        )
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
        initial.nodes.insert(
            node("n3"),
            ClusterNode::new(node("n3"), "127.0.0.1:1", 1, 1_000_000, 1).unwrap(),
        );
        initial.generation += 1;
        initial.validate().unwrap();
        let mut store = store;
        store
            .install_authoritative_topology(initial.clone())
            .unwrap();
        let mut db = Vec::new();
        let mut services = Vec::new();
        for index in 0..4 {
            let directory = root.path().join(format!("node-{index}"));
            let database_config = DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
                .with_required_commit_admission(true);
            let mut created = BicDb::open_with_config(&directory, database_config.clone()).unwrap();
            created
                .create_collection(CLUSTER_EVENTS_COLLECTION)
                .unwrap();
            created.close().unwrap();
            let database = Arc::new(RwLock::new(
                BicDb::open_with_config(&directory, database_config).unwrap(),
            ));
            let service = Arc::new(Mutex::new(
                ClusterDataNodeService::open(
                    config.cluster_id.clone(),
                    node(&format!("n{index}")),
                    directory,
                    Arc::clone(&database),
                    false,
                )
                .unwrap(),
            ));
            db.push(database);
            services.push(service);
        }
        let bus = Arc::new(Bus {
            topology: Arc::new(RwLock::new(initial.clone())),
            nodes: (0..4)
                .map(|i| (node(&format!("n{i}")), Arc::clone(&services[i])))
                .collect(),
            available: RwLock::new([node("n0"), node("n1"), node("n2"), node("n3")].into()),
        });
        Self {
            _root: root,
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
    fn commit(&self, index: usize, key: &str, value: &Value) {
        let database = self.db[index].read();
        let mut transaction = database.begin_transaction().unwrap();
        transaction
            .insert(
                CLUSTER_EVENTS_COLLECTION,
                Record::new(format!("fixture/{key}")).with_metadata(value.clone()),
            )
            .unwrap();
        transaction.commit().unwrap();
    }
}

#[derive(Debug)]
struct RelocationBus(Arc<Bus>);
impl ClusterRelocationTransport for RelocationBus {
    fn prepare_learner(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        cancellation.check()?;
        let source = self.0.service(&range.leader)?.lock().database();
        let bundle = source.read().cluster_schema_bundle()?;
        let target = self.0.service(&relocation.target)?;
        let mut target = target.lock();
        target.install_schema_bundle(relocation, range, &bundle)?;
        target.prepare_learner(relocation)
    }
    fn copy_snapshot_step(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        options: &RangeSnapshotOptions,
        cancellation: &CancellationToken,
    ) -> Result<SnapshotCopyProgress> {
        cancellation.check()?;
        let step = self
            .0
            .service(&range.leader)?
            .lock()
            .export_snapshot_step(relocation, range, options)?;
        self.0
            .service(&relocation.target)?
            .lock()
            .apply_snapshot_step(relocation, range, &step)
    }
    fn catch_up_step(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        max: usize,
        cancellation: &CancellationToken,
    ) -> Result<CatchUpProgress> {
        cancellation.check()?;
        let durable = self
            .0
            .service(&relocation.target)?
            .lock()
            .learner_durable_commit_sequence(relocation)?;
        let batch = self
            .0
            .service(&range.leader)?
            .lock()
            .export_catch_up(range, durable, max)?;
        self.0
            .service(&relocation.target)?
            .lock()
            .apply_catch_up(relocation, range, &batch)
    }
    fn cleanup_source(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        resume: Option<&str>,
        max: usize,
        cancellation: &CancellationToken,
    ) -> Result<CleanupProgress> {
        cancellation.check()?;
        let source = relocation.source.as_ref().unwrap();
        let result = self
            .0
            .service(source)?
            .lock()
            .cleanup_source_step(relocation, range, resume, max)?;
        if result.completed {
            self.0
                .service(&relocation.target)?
                .lock()
                .complete_relocation(relocation)?;
        }
        Ok(result)
    }
}

fn relocation_image_case(commands: usize, expect_history_available: bool) {
    let f = Fixture::new();
    let old = f.coordinator(0, Arc::new(RwLock::new(f.initial.clone())));
    old.recover_local_leadership(1).unwrap();
    for index in 1..=commands {
        f.commit(0, "image", &json!({"last_command":index}));
    }
    for database in &f.db {
        database.read().clear_commit_admission();
    }
    drop(old);
    let before: Vec<_> = (0..4)
        .map(|i| {
            f.services[i]
                .lock()
                .range_write_progress(RangeId::new(1).unwrap())
        })
        .collect();
    let mut store =
        DistributionStore::open(f._root.path().join("metadata"), f.config.clone(), false).unwrap();
    let initial = store.topology().ranges.values().next().unwrap().clone();
    let plan = RebalancePlan {
        id: format!("actual-image-{commands}"),
        topology_generation: store.topology().generation,
        created_at_ms: 10,
        replica_moves: vec![PlannedReplicaMove {
            range_id: initial.id,
            expected_epoch: initial.epoch,
            source: Some(node("n0")),
            target: node("n3"),
            estimated_bytes: 128,
            reason: ReplicaMoveReason::LoadBalance,
        }],
        leader_transfers: Vec::new(),
        unplaced_ranges: Vec::new(),
        estimated_bytes_in_flight: 128,
    };
    let id = store.apply_rebalance_plan(&plan, &node("n0"), 10).unwrap()[0];
    let mut driver = TransportClusterRelocationDriver::new(
        RelocationBus(Arc::clone(&f.bus)),
        ClusterRelocationTransportConfig {
            snapshot: RangeSnapshotOptions {
                max_records_per_batch: 1,
                max_bytes_per_batch: 1024 * 1024,
                max_record_bytes: 1024 * 1024,
            },
            max_commit_frames_per_step: 128,
        },
    )
    .unwrap();
    let mut phases = Vec::new();
    let mut reopened_phases = std::collections::BTreeSet::new();
    for step in 0..40 {
        *f.bus.topology.write() = store.topology().clone();
        let phase = store.relocation(id).unwrap().phase;
        if phase == RelocationPhase::Completed {
            break;
        }
        let outcome = driver
            .drive_relocation(
                &mut store,
                id,
                &node("n0"),
                11 + step,
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let after = store.relocation(id).unwrap().phase;
        let learner = RangeLearnerStore::open(
            f._root.path().join("node-3"),
            f.config.cluster_id.clone(),
            false,
        )
        .unwrap()
        .state(id)
        .cloned();
        let payload = f.db[3]
            .read()
            .get(CLUSTER_EVENTS_COLLECTION, "fixture/image")
            .unwrap()
            .map(|r| r.metadata.clone());
        let premature = if matches!(
            after,
            RelocationPhase::SnapshotCopying | RelocationPhase::CatchingUp
        ) {
            Some(
                store
                    .promote_relocation(id, &node("n0"), 60 + step)
                    .unwrap_err()
                    .to_string(),
            )
        } else {
            None
        };
        phases.push(json!({"step":step,"before":phase,"after":after,"outcome":format!("{outcome:?}"),"learner":learner,"target_payload":payload,"target_range_log":f.services[3].lock().range_write_progress(initial.id),"premature_promotion_error":premature}));
        if matches!(
            after,
            RelocationPhase::SnapshotCopying | RelocationPhase::CatchingUp
        ) && store.relocation(id).unwrap().snapshot_bytes_copied > 0
            && reopened_phases.insert(format!("{after:?}"))
        {
            let checkpoint = serde_json::to_value(store.relocation(id).unwrap()).unwrap();
            drop(driver);
            drop(store);
            store =
                DistributionStore::open(f._root.path().join("metadata"), f.config.clone(), false)
                    .unwrap();
            assert_eq!(
                serde_json::to_value(store.relocation(id).unwrap()).unwrap(),
                checkpoint
            );
            driver = TransportClusterRelocationDriver::new(
                RelocationBus(Arc::clone(&f.bus)),
                ClusterRelocationTransportConfig {
                    snapshot: RangeSnapshotOptions {
                        max_records_per_batch: 1,
                        max_bytes_per_batch: 1024 * 1024,
                        max_record_bytes: 1024 * 1024,
                    },
                    max_commit_frames_per_step: 128,
                },
            )
            .unwrap();
        }
    }
    assert_eq!(
        store.relocation(id).unwrap().phase,
        RelocationPhase::Completed
    );
    let topology = store.topology().clone();
    *f.bus.topology.write() = topology.clone();
    let expected = json!({"last_command":commands});
    assert_eq!(
        f.db[3]
            .read()
            .get(CLUSTER_EVENTS_COLLECTION, "fixture/image")
            .unwrap()
            .unwrap()
            .metadata,
        expected
    );
    assert!(
        f.db[0]
            .read()
            .get(CLUSTER_EVENTS_COLLECTION, "fixture/image")
            .unwrap()
            .is_none(),
        "supported cleanup must actually remove old source rows"
    );
    let installed = f.services[3].lock().range_write_progress(initial.id);
    assert_eq!(
        installed.resolved_through, 0,
        "snapshot currently transfers no range-log base"
    );
    let next = f.coordinator(3, Arc::new(RwLock::new(topology.clone())));
    let recovery = next.recover_local_leadership(1);
    let after = f.services[3].lock().range_write_progress(initial.id);
    let value_after_recovery = f.db[3]
        .read()
        .get(CLUSTER_EVENTS_COLLECTION, "fixture/image")
        .unwrap()
        .unwrap()
        .metadata
        .clone();
    assert_eq!(value_after_recovery, expected);
    println!(
        "{}",
        json!({"case":commands,"commits_returned_success":commands,"before_relocation":before,"relocation_phases":phases,"final_topology":topology,"target_payload":expected,"target_payload_after_recovery":value_after_recovery,"control_plane_reopened_phases":reopened_phases,"target_range_log_before_recovery":installed,"target_range_log_after_recovery":after,"recovery":format!("{recovery:?}"),"history_available_expected":expect_history_available,"physical_ha_qualification":false})
    );
    if expect_history_available {
        assert_eq!(recovery.unwrap(), 1);
        assert_eq!(after.resolved_through, commands as u64);
    } else {
        assert!(recovery
            .unwrap_err()
            .to_string()
            .contains("requires a range snapshot"));
    }
    for database in &f.db {
        database.read().clear_commit_admission();
    }
}

#[test]
fn supported_relocation_below_retention_recovers_new_leader_log() {
    relocation_image_case(8, true);
}
#[test]
fn supported_relocation_after_compaction_has_rows_but_no_recoverable_image_floor() {
    relocation_image_case(1025, false);
}
