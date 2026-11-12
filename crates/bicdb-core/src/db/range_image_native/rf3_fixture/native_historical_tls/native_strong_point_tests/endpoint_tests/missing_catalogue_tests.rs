//! Copy-only missing-decision-catalogue baseline. No production guard is changed.
//! Actual TLS votes/metadata, APP ACK and real service CQ precede the fault.
use super::*;
use crate::distribution_native_apply::{NativeOriginStatusRequest, NativeReplicaApplyObservation};
use crate::distribution_range_consensus::{
    NativeRangeCommitCertificate, RangeWriteAck, RangeWriteCommand, RangeWriteProbe,
    RangeWriteProgress, RangeWriteRepairBatch, RangeWriteRepairLimits, RangeWriteState,
    RangeWriteTransport,
};
use crate::distribution_range_recovery::{NativeCompletionRequest, NativeReadyRequest};
use crate::BicDbError;
use sha2::{Digest, Sha256};

#[derive(Debug)]
struct Ack01Transport {
    inner: Arc<TcpClusterRelocationTransport>,
}
impl Ack01Transport {
    fn reachable(&self, id: &ClusterNodeId) -> Result<()> {
        if id == &node("n2") {
            return Err(invalid("test drops data delivery to clean n2"));
        }
        Ok(())
    }
}
impl RangeWriteTransport for Ack01Transport {
    fn validate_native_command_pre_admission(
        &self,
        voters: &[ClusterNodeId],
        command: &RangeWriteCommand,
    ) -> Result<()> {
        self.inner
            .validate_native_command_pre_admission(voters, command)
    }
    fn validate_native_origin_pre_admission(
        &self,
        voters: &[ClusterNodeId],
        command: &RangeWriteCommand,
        intent: &crate::distribution_range_consensus::NativeRangeCommandIntent,
    ) -> Result<()> {
        self.inner
            .validate_native_origin_pre_admission(voters, command, intent)
    }
    fn observe_native_origin(
        &self,
        id: &ClusterNodeId,
        request: &NativeOriginStatusRequest,
        parent: &CancellationToken,
    ) -> Result<NativeReplicaApplyObservation> {
        self.reachable(id)?;
        self.inner.observe_native_origin(id, request, parent)
    }
    fn recertify_native_origin(
        &self,
        id: &ClusterNodeId,
        command: &RangeWriteCommand,
        parent: &CancellationToken,
    ) -> Result<RangeWriteAck> {
        self.reachable(id)?;
        self.inner.recertify_native_origin(id, command, parent)
    }
    fn apply_native_origin(
        &self,
        id: &ClusterNodeId,
        _command: &RangeWriteCommand,
        _certificate: &NativeRangeCommitCertificate,
        parent: &CancellationToken,
    ) -> Result<NativeReplicaApplyObservation> {
        parent.check()?;
        self.reachable(id)?;
        Err(invalid(
            "test drops NativeApply to n1 AFTER genuine CQ persistence",
        ))
    }
    fn range_write_progress(
        &self,
        id: &ClusterNodeId,
        range: RangeId,
        epoch: u64,
    ) -> Result<RangeWriteProgress> {
        self.reachable(id)?;
        self.inner.range_write_progress(id, range, epoch)
    }
    fn probe_range_write(
        &self,
        id: &ClusterNodeId,
        range: RangeId,
        epoch: u64,
        index: u64,
    ) -> Result<RangeWriteProbe> {
        self.reachable(id)?;
        self.inner.probe_range_write(id, range, epoch, index)
    }
    fn prepare_range_write(
        &self,
        id: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.reachable(id)?;
        self.inner.prepare_range_write(id, command)
    }
    fn commit_range_write(
        &self,
        id: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.reachable(id)?;
        self.inner.commit_range_write(id, command)
    }
    fn certify_range_write(
        &self,
        id: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.reachable(id)?;
        self.inner.certify_range_write(id, command)
    }
    fn apply_range_write(
        &self,
        id: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.reachable(id)?;
        self.inner.apply_range_write(id, command)
    }
    fn abort_range_write(
        &self,
        id: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.reachable(id)?;
        self.inner.abort_range_write(id, command)
    }
    fn fetch_range_write_repair(
        &self,
        id: &ClusterNodeId,
        range: RangeId,
        epoch: u64,
        after: u64,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        self.reachable(id)?;
        self.inner
            .fetch_range_write_repair(id, range, epoch, after, limits)
    }
    fn apply_range_write_repair(
        &self,
        id: &ClusterNodeId,
        batch: &RangeWriteRepairBatch,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        self.reachable(id)?;
        self.inner.apply_range_write_repair(id, batch, limits)
    }
}
fn save(root: &Path, report: &Value) {
    let bytes = serde_json::to_vec_pretty(report).unwrap();
    let file = root.join("missing-catalogue-result.json");
    std::fs::write(&file, &bytes).unwrap();
    std::fs::File::open(&file).unwrap().sync_all().unwrap();
    eprintln!(
        "NATIVE_MISSING_CATALOGUE {}",
        String::from_utf8(bytes).unwrap()
    );
}
fn native_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}

/// Strict actual-copy reopen. There is no factory/genesis, catalogue rewrite,
/// recovered-epoch seed, metadata reset, or native-floor setter in this path.
fn reopen_stopped(
    root: tempfile::TempDir,
    keys: tempfile::TempDir,
    saved: ClusterTopology,
    specs: BTreeMap<ClusterNodeId, RangeImageBirthSpec>,
) -> std::result::Result<EnforcedNetwork, (&'static str, String)> {
    let error = |stage| move |e: BicDbError| (stage, e.to_string());
    let mut databases = BTreeMap::new();
    for (id, member) in &saved.nodes {
        let (db, _) = BicDb::open_range_image_database(
            &root.path().join(id.as_str()),
            native_config(),
            &specs[id],
        )
        .map_err(error("strict_native_open_refused"))?;
        let actual = db
            .native_recovery_admission(
                &saved.cluster_id,
                saved.range_image_bootstrap_id.unwrap(),
                id,
                member.incarnation,
            )
            .map_err(error("strict_native_identity_refused"))?;
        let birth = crate::distribution::NativeStoreBirthV1::from_actual_native(&actual)
            .map_err(error("strict_native_identity_refused"))?;
        if member.native_store_birth_v1.as_ref() != Some(&birth) {
            return Err((
                "strict_native_identity_refused",
                "copied actual birth differs from saved registry".into(),
            ));
        }
        let schema = db
            .schema_compatibility_fingerprint()
            .map_err(error("strict_native_schema_refused"))?;
        if member
            .labels
            .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
            != Some(&schema.sha256)
        {
            return Err((
                "strict_native_schema_refused",
                "copied actual schema differs".into(),
            ));
        }
        databases.insert(id.clone(), Arc::new(RwLock::new(db)));
    }
    let topology = Arc::new(RwLock::new(saved.clone()));
    let mut services = BTreeMap::new();
    for (id, db) in &databases {
        let service = ClusterDataNodeService::open(
            saved.cluster_id.clone(),
            id.clone(),
            db.read().data_path(),
            db.clone(),
            true,
        )
        .map_err(error("strict_service_open_refused"))?;
        services.insert(id.clone(), Arc::new(Mutex::new(service)));
    }
    let transport = Arc::new(ActualNativeRf3ServiceTransport {
        caller: node("n0"),
        topology: topology.clone(),
        services: services.clone(),
        observations: Mutex::new(Vec::new()),
        unavailable: Mutex::new(BTreeSet::new()),
        native_delivery: AtomicBool::new(false),
        native_drop_once: Mutex::new(None),
        lose_commit_replies_after_durability: AtomicBool::new(false),
    });
    let coordinator = Arc::new(
        RangeWriteCoordinator::new_schema_fenced(
            saved.cluster_id.clone(),
            node("n0"),
            topology.clone(),
            services[&node("n0")].clone(),
            transport.clone(),
        )
        .map_err(error("coordinator_reopen_refused"))?,
    );
    let native = ActualNativeRf3Fixture {
        root,
        topology: topology.clone(),
        databases,
        services,
        specs,
        fresh_factories: BTreeMap::new(),
        transport,
        coordinator,
    };
    let config = |id: &ClusterNodeId| ClusterNetworkTransportConfig {
        connect_timeout_ms: 50,
        io_timeout_ms: 4000,
        max_frame_bytes: 4 * 1024 * 1024,
        max_inbound_connections: 16,
        dev_localhost_plaintext: false,
        tls: Some(ReplicationTlsConfig {
            cert_path: keys.path().join(format!("{}.pem", id.as_str())),
            key_path: keys.path().join(format!("{}.key", id.as_str())),
            ca_path: keys.path().join("ca.pem"),
            require_client_cert: true,
            dev_localhost_plaintext: false,
        }),
    };
    let mut clients = BTreeMap::new();
    let mut metadata = BTreeMap::new();
    for id in saved.nodes.keys() {
        clients.insert(
            id.clone(),
            Arc::new(
                TcpClusterRelocationTransport::new(
                    saved.cluster_id.clone(),
                    id.clone(),
                    topology.clone(),
                    config(id),
                )
                .map_err(error("actual_tls_reopen_refused"))?,
            ),
        );
        let store = MetadataConsensusStore::reopen_native_owned(
            keys.path().join(format!("strong-meta-{id}")),
            native.databases[id].clone(),
            id.clone(),
            saved.clone(),
            &CancellationToken::uncancelable(),
        )
        .map_err(error("strict_owned_metadata_reopen_refused"))?;
        metadata.insert(id.clone(), Arc::new(Mutex::new(store)));
    }
    let mut providers = BTreeMap::new();
    let mut runtimes = BTreeMap::new();
    for id in saved.nodes.keys() {
        let provider = Arc::new(
            MetadataReadProvider::new(metadata[id].clone(), clients[id].clone())
                .map_err(error("provider_reopen_refused"))?,
        );
        runtimes.insert(
            id.clone(),
            Arc::new(
                RangeRecoveryRuntime::new(native.services[id].clone(), provider.clone())
                    .with_participant_transport(clients[id].clone()),
            ),
        );
        providers.insert(id.clone(), provider);
    }
    let mut dispatchers = BTreeMap::new();
    let mut coordinators = BTreeMap::new();
    for id in saved.nodes.keys() {
        dispatchers.insert(
            id.clone(),
            crate::distribution_native_read::NativeStrongEventDispatcher::new(
                &native.services[id],
                &runtimes[id],
                &clients[id],
                &topology,
            )
            .map_err(error("dispatcher_reopen_refused"))?,
        );
        coordinators.insert(
            id.clone(),
            Arc::new(
                RangeWriteCoordinator::new_schema_fenced(
                    saved.cluster_id.clone(),
                    id.clone(),
                    topology.clone(),
                    native.services[id].clone(),
                    clients[id].clone(),
                )
                .map_err(error("coordinator_reopen_refused"))?,
            ),
        );
    }
    let mut servers = Vec::new();
    // Old data/native ACK holder0 is genuinely unavailable: no endpoint0 is
    // started. Its copied R1/row remain intact and are never used as an ACK.
    for id in [node("n1"), node("n2")] {
        servers.push(
            start_cluster_data_server_with_recovery_runtime(
                &saved.nodes[&id].address,
                config(&id),
                topology.clone(),
                native.services[&id].clone(),
                None,
                Some(metadata[&id].clone()),
                None,
                Some(providers[&id].clone()),
                Some(runtimes[&id].clone()),
            )
            .map_err(error("survivor_listener_reopen_refused"))?,
        );
    }
    Ok(EnforcedNetwork {
        native,
        clients,
        metadata,
        providers,
        runtimes,
        coordinators,
        hosts: BTreeMap::new(),
        servers,
        dispatchers,
        _keys: keys,
    })
}

fn replicate_survivor_metadata(f: &EnforcedNetwork) {
    for _ in 0..2 {
        let request = f.metadata[&node("n1")]
            .lock()
            .append_request_for_peer(&node("n2"), 16)
            .unwrap();
        let response = f.clients[&node("n1")]
            .append_metadata(&node("n2"), request)
            .unwrap();
        f.metadata[&node("n1")]
            .lock()
            .record_append_response(&node("n2"), &response)
            .unwrap();
    }
}
fn elect_survivors_and_commit_e2(f: &EnforcedNetwork, old: &RangeDescriptor) -> RangeDescriptor {
    let request = f.metadata[&node("n1")].lock().start_election().unwrap();
    let reply = f.clients[&node("n1")]
        .request_metadata_vote(&node("n2"), request.clone())
        .unwrap();
    assert_eq!(reply.voter_id, node("n2"));
    assert_eq!(reply.term, request.term);
    assert!(reply.vote_granted);
    f.metadata[&node("n1")]
        .lock()
        .become_leader(&BTreeSet::from([node("n1"), reply.voter_id]))
        .unwrap();
    replicate_survivor_metadata(f);
    let mut current = f.metadata[&node("n1")].lock().committed_topology().clone();
    current.generation += 1;
    let selected = current
        .ranges
        .values_mut()
        .find(|r| r.id == old.id)
        .unwrap();
    selected.epoch += 1;
    selected.leader = node("n2");
    let range = selected.clone();
    let entry = f.metadata[&node("n1")]
        .lock()
        .propose_topology(current.clone())
        .unwrap();
    replicate_survivor_metadata(f);
    assert!(f.metadata[&node("n1")].lock().status().commit_index >= entry.index);
    assert_eq!(
        f.metadata[&node("n2")].lock().committed_topology(),
        &current
    );
    *f.native.topology.write() = current;
    range
}
fn prepare_survivor_scope(f: &EnforcedNetwork, range: &RangeDescriptor) -> RecoveryFenceScope {
    let actor = f.clients[&node("n2")].local_metadata_read_participant();
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let nonce = Uuid::now_v7();
    let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let witness = f.providers[&node("n2")]
        .witness_recovery_role(
            &actor,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            purpose.clone(),
            &parent,
        )
        .unwrap();
    let role = RecoveryRoleAdmission::from_witness(
        witness,
        &actor,
        &node("n2"),
        business,
        range.id,
        range.epoch,
        nonce,
        challenge,
        &purpose,
    )
    .unwrap();
    let mut round = RecoveryFenceQuorumRound::begin(role, &parent).unwrap();
    let scope = round.scope().clone();
    for id in [node("n1"), node("n2")] {
        let observed = f.clients[&node("n2")]
            .recovery_epoch_fence_observation(
                &id,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 3500,
                },
                &parent,
            )
            .unwrap();
        round.record(Arc::new(observed)).unwrap();
    }
    round.finish().unwrap().require_live().unwrap();
    f.idle();
    scope
}
fn activate_survivors(
    f: &EnforcedNetwork,
    range: &RangeDescriptor,
    scope: &RecoveryFenceScope,
) -> u64 {
    let mut actual_cut = None;
    for id in [node("n1"), node("n2")] {
        let ready = f.clients[&node("n2")]
            .prepare_native_ready_observation(
                &id,
                &NativeReadyRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        if let Some(cut) = actual_cut {
            assert_eq!(ready.reply().range_index, cut);
        } else {
            actual_cut = Some(ready.reply().range_index);
        }
        f.idle();
    }
    for id in [node("n1"), node("n2")] {
        let completed = f.clients[&node("n2")]
            .complete_native_recovery_observation(
                &id,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        assert_eq!(completed.reply().range_index, actual_cut.unwrap());
        f.idle();
    }
    let actor = f.clients[&node("n2")].local_metadata_read_participant();
    for id in [node("n1"), node("n2")] {
        f.runtimes[&id]
            .activate_native_recovery(
                &actor,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        f.idle();
    }
    f.coordinators[&range.leader]
        .recover_local_leadership(8)
        .unwrap();
    f.dispatchers[&range.leader]
        .publish(&f.coordinators[&range.leader], &f.hosts[&range.leader])
        .unwrap();
    actual_cut.unwrap()
}

fn replay_only_actual_discovered_history(
    f: &EnforcedNetwork,
    scope: &RecoveryFenceScope,
) -> Option<String> {
    // Index1 is a probe, never a supplied decision/floor. The missing case may
    // NOT replay the retained external ledger's command as fabricated proof.
    let mut selected = None;
    for id in [node("n1"), node("n2")] {
        let history = f.clients[&node("n2")]
            .recovery_fenced_history_observation(
                &id,
                &RecoveryFencedProbeRequest {
                    scope: scope.clone(),
                    index: Some(1),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        if let Some(entry) = history.entry() {
            assert!(matches!(
                entry.state,
                RangeWriteState::QuorumCommitted | RangeWriteState::Applied
            ));
            if let Some(command) = &selected {
                assert_eq!(command, &entry.command);
            } else {
                selected = Some(entry.command.clone());
            }
        }
        f.idle();
    }
    let command = selected?;
    let checksum = command.checksum_sha256.clone();
    let request = RecoveryHistoricalCommandRequest {
        scope: scope.clone(),
        command,
        budget_ms: 3500,
    };
    for apply in [false, true] {
        for id in [node("n1"), node("n2")] {
            f.clients[&node("n2")]
                .recovery_historical_command_observation(
                    &id,
                    &request,
                    apply,
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            f.idle();
        }
    }
    Some(checksum)
}
fn exercise_cold_path(
    f: &mut EnforcedNetwork,
    old: &RangeDescriptor,
    key: &str,
    report: &mut Value,
) -> NativeEventGetReply {
    report["stage"] = json!("actual_survivor_election_and_e2");
    save(f.native.root.path(), report);
    let range = elect_survivors_and_commit_e2(f, old);
    report["range_epoch"] = json!(range.epoch);
    report["stage"] = json!("actual_fresh_host_policy");
    save(f.native.root.path(), report);
    for id in [node("n1"), node("n2")] {
        f.refresh_endpoint_host(id);
    }
    report["stage"] = json!("actual_fenced_survivor_quorum");
    save(f.native.root.path(), report);
    let scope = prepare_survivor_scope(f, &range);
    report["stage"] = json!("actual_discovered_history_replay");
    save(f.native.root.path(), report);
    report["discovered_command_sha256"] = json!(replay_only_actual_discovered_history(f, &scope));
    report["stage"] = json!("actual_prepare_complete_activate");
    save(f.native.root.path(), report);
    let cut = activate_survivors(f, &range, &scope);
    report["actual_prepared_cut"] = json!(cut);
    report["stage"] = json!("actual_strong_get");
    save(f.native.root.path(), report);
    let response = f.wire_get("n2", &f.event_request(key, &range));
    f.idle();
    response
}
struct ActualStoppedOwner {
    database: std::sync::Weak<()>,
    directory: std::sync::Weak<bicdb_page::DirectoryLock>,
}
fn stopped_owner_weaks(f: &EnforcedNetwork) -> Vec<ActualStoppedOwner> {
    f.native
        .databases
        .values()
        .map(|database| {
            let database = database.read();
            ActualStoppedOwner {
                database: database.native_metadata_owner_open_lifetime(),
                directory: Arc::downgrade(database._directory_lock.as_ref().unwrap()),
            }
        })
        .collect()
}
fn require_all_owners_ended(owners: &[ActualStoppedOwner]) {
    let until = Instant::now() + Duration::from_secs(5);
    while owners
        .iter()
        .any(|owner| owner.database.upgrade().is_some() || owner.directory.upgrade().is_some())
    {
        assert!(Instant::now() < until, "ALL actual DB/directory/MStore/service/coordinator/IO owners must end before any copy or alias reopen");
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn native_event_endpoint_missing_catalogue_same_birth_restart_must_never_return_acked_absence() {
    // This constructor variant gathers ACTUAL TLS votes/Append ACKs for the
    // initial metadata barrier. Existing declared-election fixtures stay intact.
    let mut original = EnforcedNetwork::new_endpoint_mode_with_election(true, true, 3, true);
    original.native.root.disable_cleanup(true);
    original._keys.disable_cleanup(true);
    let (range, key) = original.event_range_key("one-real-ack-before-catalogue-loss");
    let expected = json!({"ack_id":"one-original-app-ack", "full":{"text":"preserve this complete value", "null":null, "array":[1,2,3]}});
    let source = node("n0");
    let faults = Arc::new(Ack01Transport {
        inner: original.clients[&source].clone(),
    });
    let origin = Arc::new(
        RangeWriteCoordinator::new_schema_fenced(
            original.native.topology.read().cluster_id.clone(),
            source.clone(),
            original.native.topology.clone(),
            original.native.services[&source].clone(),
            faults.clone(),
        )
        .unwrap(),
    );
    let business = Uuid::now_v7();
    let host_mq = original.clients[&source]
        .acquire_host_metadata_observation(
            &node("n1"),
            business,
            Duration::from_secs(5),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let policy = {
        let service = original.native.services[&source].lock();
        let db = original.native.databases[&source].read();
        service
            .prepare_native_host_writer_enrollment(&db, host_mq, business)
            .unwrap()
    };
    let host = original.native.databases[&source]
        .read()
        .install_enrolled_native_coordinator(origin.clone(), policy)
        .unwrap();
    original.hosts.insert(source.clone(), Arc::new(host));
    origin.recover_local_leadership(8).unwrap();
    // Real outer TLS APP request/ACK; Prepare/Commit/Certify for n1 are also
    // genuine TLS/service persistence. Faults only drop delivery, never ACK.
    original.append_event(&key, expected.clone());
    original.idle();
    let record_id = crate::cluster_event_record_id("tenant", &key).unwrap();
    assert_eq!(
        original.native.databases[&source]
            .read()
            .get(crate::CLUSTER_EVENTS_COLLECTION, &record_id)
            .unwrap()
            .unwrap()
            .metadata,
        expected
    );
    assert_eq!(
        original.native.databases[&source]
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        1
    );
    let cq = original.native.services[&node("n1")]
        .lock()
        .probe_range_write_for_peer(&range, &source, 1)
        .unwrap();
    let command = cq.entry.unwrap();
    assert_eq!(command.state, RangeWriteState::QuorumCommitted);
    assert_eq!(cq.progress.last_index, 1);
    assert_eq!(
        original.native.databases[&node("n1")]
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        0
    );
    let clean = original.native.services[&node("n2")]
        .lock()
        .probe_range_write_for_peer(&range, &source, 1)
        .unwrap();
    assert!(clean.entry.is_none());
    assert_eq!(clean.progress.last_index, 0);
    assert_eq!(
        original.native.databases[&node("n2")]
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        0
    );
    let data_source = original.native.root.path().to_path_buf();
    let keys_source = original._keys.path().to_path_buf();
    let saved = original.metadata[&node("n1")]
        .lock()
        .committed_topology()
        .clone();
    let specs = original.native.specs.clone();
    let original_weaks = stopped_owner_weaks(&original);
    let report = json!({"version":1,"stage":"original_actual_app_ack", "original_data":data_source, "original_private_metadata_tls":keys_source,
        "range_id":range.id.get(),"original_epoch":range.epoch,"key":key,"record_id":record_id,
        "ack_ledger":[{"id":"one-original-app-ack", "range_index":1,"command_sha256":command.command.checksum_sha256,"value_sha256":hex::encode(Sha256::digest(serde_json::to_vec(&expected).unwrap()))}],
        "original_n1":"durable_CQ1_native0", "original_n2":"raw0_native0"});
    save(&data_source, &report);
    drop(origin);
    drop(faults);
    drop(original);
    require_all_owners_ended(&original_weaks);
    // Two independent stopped copies from the SAME original cut. Original
    // keys/databases are retained and are never reopened or edited.
    let mut intact_data = ActualNativeRf3Fixture::copy_stopped_recovery_tree(&data_source).unwrap();
    let mut intact_keys = ActualNativeRf3Fixture::copy_stopped_recovery_tree(&keys_source).unwrap();
    let mut missing_data =
        ActualNativeRf3Fixture::copy_stopped_recovery_tree(&data_source).unwrap();
    let mut missing_keys =
        ActualNativeRf3Fixture::copy_stopped_recovery_tree(&keys_source).unwrap();
    for root in [
        &mut intact_data,
        &mut intact_keys,
        &mut missing_data,
        &mut missing_keys,
    ] {
        root.disable_cleanup(true);
    }
    let intact_path = intact_data.path().to_path_buf();
    let missing_path = missing_data.path().to_path_buf();
    let catalog = missing_path
        .join("n1")
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    let before = std::fs::read(&catalog).unwrap();
    let encoded: Value = serde_json::from_slice(&before).unwrap();
    assert_eq!(encoded["catalog"]["node_id"], "n1");
    assert_eq!(
        encoded["catalog"]["ranges"][range.id.get().to_string()]["entries"]["1"]["state"],
        "quorum_committed"
    );
    let intact_catalog = intact_path
        .join("n1")
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    assert_eq!(std::fs::read(intact_catalog).unwrap(), before);
    let original_catalog = data_source
        .join("n1")
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    assert_eq!(std::fs::read(original_catalog).unwrap(), before);
    let mut intact_report = report.clone();
    intact_report["case"] = json!("INTACT_CONTROL");
    let mut intact = reopen_stopped(intact_data, intact_keys, saved.clone(), specs.clone())
        .unwrap_or_else(|(stage, e)| {
            intact_report["stage"] = json!(stage);
            intact_report["error"] = json!(e);
            save(&intact_path, &intact_report);
            panic!("intact control strict open failed at {stage}: {e}")
        });
    let control_response = exercise_cold_path(&mut intact, &range, &key, &mut intact_report);
    intact_report["stage"] = json!("actual_typed_get_control_verdict");
    intact_report["typed_reply"] = serde_json::to_value(&control_response).unwrap();
    intact_report["classification"] = json!("CONTROL_MUST_PRODUCE_ACKED_VALUE");
    save(&intact_path, &intact_report);
    let (control_value, audit) = actual_value(control_response);
    assert_eq!(control_value, Some(expected.clone()), "INTACT control MUST return the complete actual acknowledged value; Unavailable is not a control success");
    assert_eq!(audit.range_index, 1);
    intact_report["classification"] = json!("INTACT_REAL_COLD_E2_STRONG_VALUE");
    intact_report["stage"] = json!("control_complete");
    save(&intact_path, &intact_report);
    let control_weaks = stopped_owner_weaks(&intact);
    drop(intact);
    require_all_owners_ended(&control_weaks);
    // Apply the ONLY fault after proving the identical intact path usable.
    std::fs::remove_file(&catalog).unwrap();
    std::fs::File::open(catalog.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
    assert!(!catalog.exists());
    let mut missing_report = report;
    missing_report["case"] = json!("MISSING_N1_CATALOGUE_ONLY");
    missing_report["intact_control"] = json!(intact_path);
    missing_report["deleted_file_sha256"] = json!(hex::encode(Sha256::digest(&before)));
    missing_report["stage"] = json!("strict_missing_copy_reopen");
    save(&missing_path, &missing_report);
    let mut missing = match reopen_stopped(missing_data, missing_keys, saved, specs) {
        Ok(f) => f,
        Err((stage, e)) => {
            // No unrelated generic-open/route error is allowed to make this
            // safety regression green. A future dedicated custody refusal can
            // be qualified explicitly after observing its actual boundary.
            missing_report["stage"] = json!(stage);
            missing_report["error"] = json!(e);
            missing_report["classification"] =
                json!("EARLY_REFUSAL_NEEDS_EXACT_CUSTODY_CLASSIFICATION");
            save(&missing_path, &missing_report);
            panic!("missing-catalogue copy refused before Strong at {stage}: {e}; qualify exact custody boundary, no unsafe-read claim");
        }
    };
    let response = exercise_cold_path(&mut missing, &range, &key, &mut missing_report);
    missing_report["stage"] = json!("actual_typed_get_verdict");
    missing_report["typed_reply"] = serde_json::to_value(&response).unwrap();
    if !matches!(&response.result, NativeEventGetResult::Value { .. }) {
        missing_report["classification"] = json!("REFUSAL_UNCLASSIFIED_NOT_A_SAFETY_PASS");
    }
    save(&missing_path, &missing_report);
    let (value, audit) = actual_value(response);
    missing_report["returned_range_index"] = json!(audit.range_index);
    missing_report["classification"] = json!(if value == Some(expected.clone()) {
        "RECOVERED_EXPECTED_VALUE"
    } else if value.is_none() {
        "UNSAFE_SUCCESSFUL_ABSENCE_AFTER_ACK"
    } else {
        "UNSAFE_SUCCESSFUL_STALE_VALUE_AFTER_ACK"
    });
    save(&missing_path, &missing_report);
    assert_eq!(value, Some(expected), "real APP ACK must never become successful absence/stale value after same-birth catalogue loss; intact twin proved this exact cold/E2/Strong path usable");
}
