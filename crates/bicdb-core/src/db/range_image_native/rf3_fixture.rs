//! Actual local RF3 origin/certificate fixture. Every data acknowledgement is
//! returned by a real fsynced ClusterDataNodeService; no canned ACK or supplied
//! certificate-voter set is used. Initial RF3 membership is a declared validated
//! test precondition, not evidence of live node joining or authenticated TCP.
use super::*;
use crate::distribution::{ClusterNode, ClusterTopology, RangeReplica, ReplicaId};
use crate::distribution_data::ClusterDataNodeService;
use crate::distribution_range_consensus::{
    RangeWriteAck, RangeWriteCommand, RangeWriteCoordinator, RangeWriteProbe, RangeWriteProgress,
    RangeWriteRepairBatch, RangeWriteRepairLimits, RangeWriteState, RangeWriteTransport,
};
use crate::{DistributionConfig, DistributionStore};

#[derive(Clone, Debug)]
pub(crate) struct ActualNativeRf3RpcObservation {
    pub(crate) operation: &'static str,
    pub(crate) destination: ClusterNodeId,
    pub(crate) command_sha256: Option<String>,
    pub(crate) state: Option<RangeWriteState>,
    pub(crate) error: Option<String>,
}

/// In-process service transport. Caller identity is the same explicit trusted
/// caller argument used by embedded APIs. It is NOT a transport-authentication
/// proof; real mTLS/metadata-role admission remains a separate integration gate.
#[derive(Debug)]
pub(crate) struct ActualNativeRf3ServiceTransport {
    caller: ClusterNodeId,
    topology: Arc<RwLock<ClusterTopology>>,
    services: BTreeMap<ClusterNodeId, Arc<Mutex<ClusterDataNodeService>>>,
    observations: Mutex<Vec<ActualNativeRf3RpcObservation>>,
    unavailable: Mutex<BTreeSet<ClusterNodeId>>,
    native_delivery: std::sync::atomic::AtomicBool,
    native_drop_once: Mutex<Option<(ClusterNodeId, u64)>>,
}
impl ActualNativeRf3ServiceTransport {
    fn range(&self, id: RangeId, epoch: u64) -> Result<RangeDescriptor> {
        self.topology
            .read()
            .range_by_id(id)
            .filter(|r| r.epoch == epoch)
            .cloned()
            .ok_or_else(|| invalid("actual RF3 fixture range/epoch differs"))
    }
    fn service(&self, id: &ClusterNodeId) -> Result<&Arc<Mutex<ClusterDataNodeService>>> {
        if self.unavailable.lock().contains(id) {
            return Err(invalid("actual RF3 fixture destination is unavailable"));
        }
        self.services
            .get(id)
            .ok_or_else(|| invalid("actual RF3 destination absent"))
    }
    fn record_ack(
        &self,
        operation: &'static str,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
        result: Result<RangeWriteAck>,
    ) -> Result<RangeWriteAck> {
        self.observations
            .lock()
            .push(ActualNativeRf3RpcObservation {
                operation,
                destination: destination.clone(),
                command_sha256: Some(command.checksum_sha256.clone()),
                state: result.as_ref().ok().map(|ack| ack.state),
                error: result.as_ref().err().map(ToString::to_string),
            });
        result
    }
    pub(crate) fn enable_native_delivery(&self) {
        self.native_delivery
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    pub(crate) fn drop_native_apply_once(&self, node: ClusterNodeId, index: u64) {
        *self.native_drop_once.lock() = Some((node, index));
    }
    pub(crate) fn set_unavailable(&self, node: &ClusterNodeId, unavailable: bool) {
        let mut nodes = self.unavailable.lock();
        if unavailable {
            nodes.insert(node.clone());
        } else {
            nodes.remove(node);
        }
    }
    pub(crate) fn observations(&self) -> Vec<ActualNativeRf3RpcObservation> {
        self.observations.lock().clone()
    }
}
impl RangeWriteTransport for ActualNativeRf3ServiceTransport {
    fn observe_native_origin(
        &self,
        node: &ClusterNodeId,
        request: &crate::distribution_native_apply::NativeOriginStatusRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_native_apply::NativeReplicaApplyObservation> {
        parent.check()?;
        let range = self.range(request.range_id, request.range_epoch)?;
        self.service(node)?.lock().observe_native_embedded_ordinary(
            &range,
            &self.caller,
            request.challenge,
        )
    }
    fn recertify_native_origin(
        &self,
        node: &ClusterNodeId,
        command: &RangeWriteCommand,
        parent: &CancellationToken,
    ) -> Result<RangeWriteAck> {
        parent.check()?;
        self.prepare_range_write(node, command)?;
        parent.check()?;
        self.commit_range_write(node, command)?;
        parent.check()?;
        self.certify_range_write(node, command)
    }
    fn apply_native_origin(
        &self,
        node: &ClusterNodeId,
        command: &RangeWriteCommand,
        certificate: &crate::distribution_range_consensus::NativeRangeCommitCertificate,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_native_apply::NativeReplicaApplyObservation> {
        parent.check()?;
        let mut loss = self.native_drop_once.lock();
        let drop_once = loss
            .as_ref()
            .is_some_and(|(n, i)| n == node && *i == command.index);
        if drop_once {
            *loss = None;
        }
        drop(loss);
        let result = if !self
            .native_delivery
            .load(std::sync::atomic::Ordering::SeqCst)
            || drop_once
        {
            Err(invalid("fixture native Apply delivery disabled; authenticated current-leader instruction required"))
        } else {
            let range = self.range(command.range_id, command.range_epoch)?;
            self.service(node)?.lock().apply_native_embedded_origin(
                &range,
                &self.caller,
                command,
                certificate,
                parent,
            )
        };
        self.observations
            .lock()
            .push(ActualNativeRf3RpcObservation {
                operation: "apply",
                destination: node.clone(),
                command_sha256: Some(command.checksum_sha256.clone()),
                state: result.as_ref().ok().map(|_| RangeWriteState::Applied),
                error: result.as_ref().err().map(ToString::to_string),
            });
        result
    }
    fn range_write_progress(
        &self,
        node: &ClusterNodeId,
        id: RangeId,
        epoch: u64,
    ) -> Result<RangeWriteProgress> {
        let range = self.range(id, epoch)?;
        self.service(node)?
            .lock()
            .range_write_progress_for_peer(&range, &self.caller)
    }
    fn apply_range_write_repair(
        &self,
        node: &ClusterNodeId,
        batch: &RangeWriteRepairBatch,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        let range = self.range(batch.range_id, batch.range_epoch)?;
        self.service(node)?
            .lock()
            .apply_range_write_repair(&range, &self.caller, batch, limits)
    }
    fn fetch_range_write_repair(
        &self,
        node: &ClusterNodeId,
        id: RangeId,
        epoch: u64,
        after: u64,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        let range = self.range(id, epoch)?;
        self.service(node)?
            .lock()
            .export_range_write_repair(&range, &self.caller, after, limits)
    }
    fn probe_range_write(
        &self,
        node: &ClusterNodeId,
        id: RangeId,
        epoch: u64,
        index: u64,
    ) -> Result<RangeWriteProbe> {
        let range = self.range(id, epoch)?;
        self.service(node)?
            .lock()
            .probe_range_write_for_peer(&range, &self.caller, index)
    }
    fn prepare_range_write(
        &self,
        node: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(command.range_id, command.range_epoch)?;
        let result =
            self.service(node)?
                .lock()
                .prepare_range_write(&range, &self.caller, command.clone());
        self.record_ack("prepare", node, command, result)
    }
    fn commit_range_write(
        &self,
        node: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(command.range_id, command.range_epoch)?;
        let result = self
            .service(node)?
            .lock()
            .commit_range_write(&range, &self.caller, command);
        self.record_ack("commit", node, command, result)
    }
    fn certify_range_write(
        &self,
        node: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(command.range_id, command.range_epoch)?;
        let result = self
            .service(node)?
            .lock()
            .certify_range_write(&range, &self.caller, command);
        self.record_ack("certify", node, command, result)
    }
    fn apply_range_write(
        &self,
        node: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(command.range_id, command.range_epoch)?;
        let result =
            self.service(node)?
                .lock()
                .apply_certified_range_write(&range, &self.caller, command);
        self.record_ack("apply", node, command, result)
    }
    fn abort_range_write(
        &self,
        node: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        let range = self.range(command.range_id, command.range_epoch)?;
        let result = self
            .service(node)?
            .lock()
            .abort_range_write(&range, &self.caller, command);
        self.record_ack("abort", node, command, result)
    }
}

pub(crate) struct ActualNativeRf3Fixture {
    pub(crate) root: tempfile::TempDir,
    pub(crate) topology: Arc<RwLock<ClusterTopology>>,
    pub(crate) databases: BTreeMap<ClusterNodeId, Arc<RwLock<BicDb>>>,
    pub(crate) services: BTreeMap<ClusterNodeId, Arc<Mutex<ClusterDataNodeService>>>,
    pub(crate) specs: BTreeMap<ClusterNodeId, RangeImageBirthSpec>,
    pub(crate) transport: Arc<ActualNativeRf3ServiceTransport>,
    pub(crate) coordinator: Arc<RangeWriteCoordinator<ActualNativeRf3ServiceTransport>>,
}
impl ActualNativeRf3Fixture {
    pub(crate) fn new() -> Self {
        Self::new_with_network_precondition(&BTreeMap::new())
    }
    /// Test-only INITIAL identities, applied before actual native birth/genesis.
    /// Never rebind an already-born database to newly invented credentials.
    pub(crate) fn new_with_network_precondition(
        network: &BTreeMap<ClusterNodeId, (String, String)>,
    ) -> Self {
        Self::new_with_network_and_initial_ranges(network, 1)
    }
    pub(crate) fn new_with_initial_ranges(initial_ranges: u32) -> Self {
        Self::new_with_network_and_initial_ranges(&BTreeMap::new(), initial_ranges)
    }
    fn new_with_network_and_initial_ranges(
        network: &BTreeMap<ClusterNodeId, (String, String)>,
        initial_ranges: u32,
    ) -> Self {
        Self::new_with_initial_enrollment(network, initial_ranges, false)
    }
    pub(crate) fn new_with_enrolled_network_precondition(
        network: &BTreeMap<ClusterNodeId, (String, String)>,
    ) -> Self {
        Self::new_with_initial_enrollment(network, 1, true)
    }
    fn new_with_initial_enrollment(
        network: &BTreeMap<ClusterNodeId, (String, String)>,
        initial_ranges: u32,
        enroll: bool,
    ) -> Self {
        assert!(matches!(initial_ranges, 1 | 2));
        let root = tempfile::tempdir().unwrap();
        let cluster = ClusterId::new("actual-native-rf3-origin").unwrap();
        let leader = ClusterNodeId::new("n0").unwrap();
        let bootstrap = Uuid::now_v7();
        let mut metadata = DistributionStore::initialize_at(
            root.path().join("metadata"),
            DistributionConfig {
                enabled: true,
                cluster_id: cluster.clone(),
                node_id: leader.clone(),
                node_address: "127.0.0.1:19000".into(),
                initial_ranges,
                replication_factor: 3,
                range_image_bootstrap_id: Some(bootstrap),
                ..Default::default()
            },
            true,
            1,
        )
        .unwrap();
        for (id, port) in [("n1", 19001), ("n2", 19002)] {
            metadata
                .join_node(
                    ClusterNode::new(
                        ClusterNodeId::new(id).unwrap(),
                        format!("127.0.0.1:{port}"),
                        1,
                        1_000_000,
                        1,
                    )
                    .unwrap(),
                    &leader,
                    1,
                )
                .unwrap();
        }
        let mut topology = metadata.topology().clone();
        // Explicit INITIAL topology fixture, before any birth/data/genesis.
        // Do not represent this as executing an online learner-promotion plan.
        for id in ["n1", "n2"] {
            for range in topology.ranges.values_mut() {
                let replica_id = ReplicaId::new(topology.next_replica_id).unwrap();
                topology.next_replica_id += 1;
                range.replicas.push(RangeReplica {
                    id: replica_id,
                    node_id: ClusterNodeId::new(id).unwrap(),
                    role: RangeReplicaRole::Voter,
                });
            }
        }
        if !network.is_empty() {
            assert_eq!(network.len(), topology.nodes.len());
            for (id, member) in &mut topology.nodes {
                let (address, pin) = &network[id];
                member.address = address.clone();
                member.tls_certificate_sha256 = Some(pin.clone());
            }
        }
        topology.validate().unwrap();
        let ranges = topology.ranges.values().cloned().collect::<Vec<_>>();
        let members = topology
            .nodes
            .iter()
            .map(|(id, node)| {
                (
                    id.clone(),
                    RangeImageBirthMember {
                        incarnation: node.incarnation,
                        tls_certificate_sha256: node.tls_certificate_sha256.clone(),
                        pending_tls_certificate_sha256: node.pending_tls_certificate_sha256.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let mut databases = BTreeMap::new();
        let mut specs = BTreeMap::new();
        let mut schema = None;
        // Explicit new-cluster fixture before any native factory, app write
        // or metadata-consensus store exists. Existing defaults are unchanged.
        if enroll {
            topology.generation = 1;
            topology.history.truncate(1);
        }
        let mut enrollments = Vec::new();
        for node in topology.nodes.values() {
            let spec = RangeImageBirthSpec {
                cluster_id: cluster.clone(),
                bootstrap_id: bootstrap,
                node_id: node.id.clone(),
                member_incarnation: node.incarnation,
                initial_members: members.clone(),
                restore_activation: None,
                initial_ranges: ranges.clone(),
            };
            let path = root.path().join(node.id.as_str());
            let (mut db, _) =
                BicDb::create_new_range_image_database(&path, config.clone(), spec.clone())
                    .unwrap();
            db.create_collection("items").unwrap();
            drop(db);
            let (db, genesis) =
                BicDb::open_range_image_database(&path, config.clone(), &spec).unwrap();
            let fingerprint = db.schema_compatibility_fingerprint().unwrap().sha256;
            if let Some(expected) = &schema {
                assert_eq!(expected, &fingerprint)
            } else {
                schema = Some(fingerprint)
            }
            let db = Arc::new(RwLock::new(db));
            let gate = db.read().range_mutation_gate();
            for range in &ranges {
                let frozen = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
                BicDb::initialize_range_image_genesis(
                    &db,
                    &genesis,
                    range,
                    &frozen,
                    Duration::from_secs(3),
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
                drop(frozen);
            }
            if enroll {
                enrollments.push(crate::distribution_native_admission::ActualNativeStoreEnrollment::from_actual_factory(
                    &genesis, &db.read(), &topology,
                ).unwrap());
            }
            databases.insert(node.id.clone(), db);
            specs.insert(node.id.clone(), spec);
        }
        for enrollment in enrollments {
            enrollment
                .bind_initial_or_unassigned_learner(&mut topology)
                .unwrap();
        }
        for node in topology.nodes.values_mut() {
            node.labels.insert(
                crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL.into(),
                schema.clone().unwrap(),
            );
        }
        let topology = Arc::new(RwLock::new(topology));
        let services = databases
            .iter()
            .map(|(id, db)| {
                let service = ClusterDataNodeService::open(
                    cluster.clone(),
                    id.clone(),
                    db.read().data_path(),
                    Arc::clone(db),
                    true,
                )
                .unwrap();
                (id.clone(), Arc::new(Mutex::new(service)))
            })
            .collect::<BTreeMap<_, _>>();
        let transport = Arc::new(ActualNativeRf3ServiceTransport {
            caller: leader.clone(),
            topology: Arc::clone(&topology),
            services: services.clone(),
            observations: Mutex::new(Vec::new()),
            unavailable: Mutex::new(BTreeSet::new()),
            native_delivery: std::sync::atomic::AtomicBool::new(false),
            native_drop_once: Mutex::new(None),
        });
        let coordinator = Arc::new(
            RangeWriteCoordinator::new_schema_fenced(
                cluster,
                leader.clone(),
                Arc::clone(&topology),
                Arc::clone(&services[&leader]),
                Arc::clone(&transport),
            )
            .unwrap(),
        );
        coordinator.recover_local_leadership(8).unwrap();
        databases[&leader]
            .read()
            .install_commit_admission(coordinator.clone());
        Self {
            root,
            topology,
            databases,
            services,
            specs,
            transport,
            coordinator,
        }
    }
    pub(crate) fn leader(&self) -> ClusterNodeId {
        self.topology
            .read()
            .ranges
            .values()
            .next()
            .unwrap()
            .leader
            .clone()
    }
    pub(crate) fn append(&self, key: &str) -> Result<()> {
        let leader = self.leader();
        let db = self.databases[&leader].write();
        let mut tx = db.begin_transaction()?;
        tx.insert("items", Record::new(key))?;
        tx.commit()
    }
}
impl Drop for ActualNativeRf3Fixture {
    fn drop(&mut self) {
        for db in self.databases.values() {
            db.read().clear_commit_admission();
        }
    }
}

#[test]
fn native_rf3_actual_services_ack_origin_while_bare_replica_apply_remains_refused() {
    let fixture = ActualNativeRf3Fixture::new();
    fixture.append("actual-rf3-origin").unwrap();
    let topology = fixture.topology.read();
    let range = topology.ranges.values().next().unwrap();
    let leader = &range.leader;
    let source = fixture.databases[leader].read();
    let lineage = source
        .current_native_range_source_lineage(range.id)
        .unwrap();
    assert_eq!(lineage.range_index(), 1);
    assert!(source.get("items", "actual-rf3-origin").unwrap().is_some());
    let observations = fixture.transport.observations();
    for node in topology.nodes.keys().filter(|node| *node != leader) {
        let rows = observations
            .iter()
            .filter(|row| &row.destination == node)
            .collect::<Vec<_>>();
        for (operation, state) in [
            ("prepare", RangeWriteState::Prepared),
            ("commit", RangeWriteState::Committed),
            ("certify", RangeWriteState::QuorumCommitted),
        ] {
            assert!(rows.iter().any(|row| row.operation == operation
                && row.state == Some(state)
                && row.error.is_none()));
        }
        assert!(rows.iter().any(|row| row.operation == "apply"
            && row.state.is_none()
            && row
                .error
                .as_deref()
                .is_some_and(|error| error.contains("authenticated current-leader instruction"))));
        let service = fixture.services[node].lock();
        let probe = service
            .probe_range_write_for_peer(range, leader, 1)
            .unwrap();
        let entry = probe.entry.unwrap();
        assert_eq!(entry.state, RangeWriteState::QuorumCommitted);
        assert_eq!(probe.progress.last_index, 1);
        assert_eq!(probe.progress.resolved_through, 0);
        let db = fixture.databases[node].read();
        assert_eq!(
            db.current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            0
        );
        assert!(db.get("items", "actual-rf3-origin").unwrap().is_none());
        assert!(rows
            .iter()
            .filter_map(|row| row.command_sha256.as_deref())
            .all(|hash| hash == entry.command.checksum_sha256));
    }
    // This is a healthy local ACK/certificate component characterization. It
    // proves neither follower materialization nor RF3 crash-recovery readiness.
}

#[test]
fn native_rf3_oversize_refuses_before_any_durable_quorum_or_native_pending() {
    let fixture = ActualNativeRf3Fixture::new();
    let leader = fixture.leader();
    let key = "oversize-before-prepare";
    let db = fixture.databases[&leader].write();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "items",
        Record::new(key).with_metadata(serde_json::json!({
            "body": "x".repeat(crate::distribution_range_consensus::MAX_NATIVE_RECOVERY_ENTRY_BYTES)
        })),
    )
    .unwrap();
    let error = tx.commit().unwrap_err();
    assert!(
        error.to_string().contains("serialized recovery payload"),
        "{error}"
    );
    drop(db);
    assert!(
        fixture.transport.observations().is_empty(),
        "no Prepare/Commit/Certify/Apply/Abort RPC may precede size refusal"
    );
    let topology = fixture.topology.read();
    let range = topology.range_for_key("items", key).unwrap();
    for (node, db) in &fixture.databases {
        let probe = fixture.services[node]
            .lock()
            .probe_range_write_for_peer(range, &leader, 1)
            .unwrap();
        assert!(probe.entry.is_none());
        assert_eq!(probe.progress.last_index, 0);
        assert_eq!(probe.progress.resolved_through, 0);
        let db = db.read();
        assert_eq!(
            db.current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            0
        );
        assert!(db.get("items", key).unwrap().is_none());
    }
    // A genuine healthy write must still ACK after this pre-decision refusal.
    drop(topology);
    fixture.append("healthy-after-oversize-refusal").unwrap();
}

#[cfg(feature = "tls-replication")]
mod native_historical_tls;

#[test]
fn actual_rf3_two_initial_ranges_have_real_genesis_and_independent_origin_indices() {
    let fixture = ActualNativeRf3Fixture::new_with_initial_ranges(2);
    let topology = fixture.topology.read().clone();
    let ranges = topology.ranges.values().collect::<Vec<_>>();
    for range in &ranges {
        for db in fixture.databases.values() {
            assert_eq!(
                db.read()
                    .current_native_range_source_lineage(range.id)
                    .unwrap()
                    .range_index(),
                0
            );
        }
    }
    for (position, range) in ranges.iter().enumerate() {
        let key = (0..)
            .map(|i| format!("actual-rf3-independent-range-{position}-{i}"))
            .find(|key| topology.range_for_key("items", key).unwrap().id == range.id)
            .unwrap();
        fixture.append(&key).unwrap();
        let db = fixture.databases[&fixture.leader()].read();
        assert_eq!(
            db.current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            1
        );
        assert!(db.get("items", &key).unwrap().is_some());
    }
    assert_ne!(
        fixture.databases[&fixture.leader()]
            .read()
            .current_commit_seq(),
        1
    );
    // Real local RF3 origin services, not ordinary follower materialization or
    // actual participant image/config activation. Native remote Apply stays refused.
}

#[test]
fn native_normal_apply_actual_rf3_services_materialize_two_acks_and_lost_apply_catchup() {
    let fixture = ActualNativeRf3Fixture::new();
    fixture.transport.enable_native_delivery();
    let lag = ClusterNodeId::new("n2").unwrap();
    fixture.transport.drop_native_apply_once(lag.clone(), 1);
    fixture.append("native-normal-first").unwrap();
    let topology = fixture.topology.read().clone();
    let range = topology.ranges.values().next().unwrap();
    assert_eq!(
        fixture.databases[&lag]
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        0
    );
    assert!(fixture.databases[&lag]
        .read()
        .get("items", "native-normal-first")
        .unwrap()
        .is_none());
    fixture.append("native-normal-second").unwrap();
    for (id, database) in &fixture.databases {
        let database = database.read();
        assert_eq!(
            database
                .current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            2,
            "{id}"
        );
        for key in ["native-normal-first", "native-normal-second"] {
            assert!(database.get("items", key).unwrap().is_some(), "{id}/{key}");
        }
        assert_eq!(
            fixture.services[id]
                .lock()
                .range_write_progress(range.id)
                .resolved_through,
            2
        );
    }
}
#[cfg(test)]
mod reopen;
